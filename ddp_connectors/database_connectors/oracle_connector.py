import oracledb
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List, Optional
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_oracle_to_postgresql_type, cast_oracle_to_typescript, normalize_ui_column_type, safe_convert_to_string
from ddp_lib.utils import serialize_if_needed


class OracleConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, schema):
        super().__init__(host, user, password, port, database)
        self.driver = "oracledb"
        self.schema = schema.upper() if schema else user.upper() # Oracle schemas are typically uppercase

    
    def ping(self) -> bool:
        """
        Tests the database connection and verifies the target schema exists.
        Returns True if successful, raises an Exception or returns False if not.
        """
        conn = None
        try:
            # 1. This tests if the Host, Port, Database, User, and Password are correct.
            # If get_connection() fails, it throws an exception immediately.
            conn = self.get_connection()
            
            with conn.cursor() as cursor:
                # 2. Basic sanity check (Ping)
                cursor.execute("SELECT 1 FROM DUAL")
                
                cursor.execute(
                    "SELECT count(*) FROM all_users WHERE username = :schema_name",
                    schema_name=self.schema
                )
                
                schema_count = cursor.fetchone()[0]
                
                if schema_count == 0:
                    logger.error(f"Ping failed: Schema '{self.schema}' does not exist.")
                    return False
                    
            logger.info(f"Successfully pinged Oracle database and verified schema: {self.schema}")
            return True
            
        except Exception as exc:
            logger.error(f"Oracle connection ping failed: {exc}")
            return False
            
        finally:
            if conn:
                conn.close()
    
    def get_connection(self):
        # automatically fetch CLOBs/BLOBs as strings/bytes
        oracledb.defaults.fetch_lobs = False
        dsn = f"{self.host}:{self.port}/{self.database}"
        conn = oracledb.connect(
            user=self.user,
            password=self.password,
            dsn=dsn
        )

        # 3. Attach our custom handler for BFILEs (and other edge cases)
        conn.outputtypehandler = self._oracle_type_handler
        
        return conn

    def _quote_identifier(self, identifier: str) -> str:
        normalized = (self._normalize_identifier(identifier) or "").upper()
        return f'"{normalized.replace(chr(34), chr(34) * 2)}"'

    def _qualify_table_sql(self, table_name: str, schema: Optional[str] = None) -> str:
        resolved_schema, resolved_table = self.resolve_schema_and_table(table_name, schema)
        if resolved_schema:
            return f"{self._quote_identifier(resolved_schema)}.{self._quote_identifier(resolved_table)}"
        return self._quote_identifier(resolved_table)

    def get_connection_schemas(self) -> List[str]:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                logger.info(
                    f"[oracle][schema_discovery] database={self.database} query_path=all_tables.owner exclude_system=true"
                )
                cursor.execute(
                    """
                    SELECT DISTINCT owner
                    FROM all_tables
                    WHERE owner NOT IN (
                        'SYS',
                        'SYSTEM',
                        'XDB',
                        'CTXSYS',
                        'MDSYS',
                        'ORDSYS',
                        'OUTLN',
                        'DBSNMP',
                        'WMSYS',
                        'APPQOSSYS',
                        'AUDSYS',
                        'GSMADMIN_INTERNAL',
                        'ANONYMOUS'
                    )
                    ORDER BY owner
                    """
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(
                f"[oracle][schema_discovery] database={self.database} query_path=all_tables.owner failed: {e}"
            )
            return []
        finally:
            conn.close()

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100, filters=None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        qualified_table = self._qualify_table_sql(table_name, schema)
        query = (
            f"SELECT * FROM {qualified_table} "
            f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        )
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cols = [c[0] for c in cursor.description]
                return [
                    {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                    for row in cursor.fetchall()
                ]
        except Exception as exc:
            logger.error(f"Error extracting batch from {qualified_table}: {exc}")
            return []
        finally:
            conn.close()

    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """
        Inserts a list of dictionaries into the specified Oracle table using executemany.
        
        Args:
            table_name (str): The target table name.
            data (List[Dict[str, Any]]): The data to insert, where each dictionary is a row.
            
        Returns:
            int: The total number of rows successfully inserted.
        """
        if not data:
            logger.warning(f"No data provided to insert into {table_name}.")
            return 0

        # 1. Extract column names from the first dictionary
        columns = list(data[0].keys())
        
        # 2. Build the parameterized SQL statement
        col_names_str = ", ".join([f'"{col}"' for col in columns])
        bind_vars_str = ", ".join([f":{i+1}" for i in range(len(columns))])
        
        # Safely handle the schema (ignore 'public') and preserve exact table case
        schema_prefix = f'"{self.schema}".' if getattr(self, 'schema', None) and self.schema.lower() != 'public' else ""
        query = f'INSERT INTO {schema_prefix}"{table_name}" ({col_names_str}) VALUES ({bind_vars_str})'

        # 3. Convert List[Dict] to List[Tuple] matching the column order
        optimized_data = [
            tuple(serialize_if_needed(val) for val in row)
            for row in data
        ]

        conn = self.get_connection()
        
        try:
            with conn.cursor() as cursor:
                cursor.executemany(query, optimized_data)
                conn.commit()
                
                inserted_count = cursor.rowcount
                logger.info(f"Successfully inserted {inserted_count} rows into {table_name}.")
                
                return inserted_count
                
        except Exception as exc:
            conn.rollback()
            logger.error(f"Error inserting data into {table_name}: {exc}")
            raise
            
        finally:
            conn.close()

    def fetch_batch(self, cursor, table_name, offset: int, batch_size: int = 100, schema: Optional[str] = None, **kwargs):
        try:
            oracledb.defaults.fetch_lobs = False
            limit = kwargs.get("limit", batch_size)
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = f"SELECT * FROM {qualified_table} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []
    
    def _oracle_type_handler(self, cursor, name, default_type, size, precision, scale):
        """
        Intercepts specific Oracle data types during fetch and converts them.
        oracledb automatically passes these 6 arguments for every column.
        """
        # Intercept BFILE types using 'default_type'
        if default_type == oracledb.DB_TYPE_BFILE:
            
            # Define how to convert the BFILE to a string
            def bfile_out_converter(bfile_obj):
                if bfile_obj and hasattr(bfile_obj, 'getfilename'):
                    dir_alias, filename = bfile_obj.getfilename()
                    return f"{dir_alias}/{filename}"
                return None
                
            # Tell the cursor to use this converter for this column
            return cursor.var(
                default_type, 
                arraysize=cursor.arraysize, 
                outconverter=bfile_out_converter
            )

    def stream_batch(self, cursor=None, table_name: Optional[str] = None, batch_size: int = 10_000, schema: Optional[str] = None):
        """Streaming for Oracle using fetchmany. Oracle driver handles arraysize natively."""
        conn = None
        managed_cursor = None
        if table_name is None and isinstance(cursor, str):
            table_name = cursor
            cursor = None

        if not table_name:
            raise ValueError("table_name is required")

        qualified_table = self._qualify_table_sql(table_name, schema)
        try:
            if cursor is None:
                conn = self.get_connection()
                managed_cursor = conn.cursor()
            else:
                managed_cursor = cursor

            managed_cursor.arraysize = batch_size
            logger.info(f"Start streaming Oracle table {qualified_table} with batch_size={batch_size}")
            managed_cursor.execute(f"SELECT * FROM {qualified_table}")

            while True:
                rows = managed_cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
            logger.info(f"Finished streaming Oracle table {qualified_table}")
        except Exception as exc:
            logger.error(f"Error streaming batch from Oracle table {table_name}: {exc}")
        finally:
            if managed_cursor and conn:
                managed_cursor.close()
            if conn:
                conn.close()

    def get_connection_tables(self, schema: Optional[str] = None):
        conn = self.get_connection()
        target_schema = (self._normalize_identifier(schema) or self.schema).upper()
        try:
            with conn.cursor() as cur:
                logger.info(
                    f"[oracle][table_discovery] database={self.database} selected_schema={target_schema} query_path=all_tables.owner"
                )
                cur.execute(
                    """
                    SELECT table_name
                    FROM all_tables
                    WHERE owner = :1
                    ORDER BY table_name
                    """,
                    [target_schema]
                )
                tables =  [row[0] for row in cur.fetchall()]
                logger.info(f"Tables: {tables}")
                return tables
        except Exception as e:
            logger.error(
                f"[oracle][table_discovery] database={self.database} selected_schema={target_schema} query_path=all_tables.owner failed: {e}"
            )
            return []
        finally:
            conn.close()

    def get_connection_columns(self, table_name: str, schema: Optional[str] = None):
        conn = self.get_connection()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        schema_name = (target_schema or self.schema).upper()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM all_tab_columns
                    WHERE owner = :1 AND table_name = :2
                    ORDER BY column_id
                    """,
                    [schema_name, pure_table.upper()]
                )
                rows = cur.fetchall()

                columns: List[Dict[str, str]] = []
                for column_name, data_type in rows:
                    ts_type = normalize_ui_column_type(cast_oracle_to_typescript(data_type))
                    columns.append({
                        "name": column_name,
                        "type": ts_type,
                        "alias": column_name,
                        "classification": "",
                    })
                return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            conn.close()

    def count_table_rows(self, table_name: str, schema: Optional[str] = None, filters=None) -> int:
        schema, _ = self.coerce_schema_and_filters(schema, filters)
        qualified_table = self._qualify_table_sql(table_name, schema)
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                sql = f"SELECT COUNT(*) FROM {qualified_table}"
                cursor.execute(sql)
                count_result = cursor.fetchone()
                return int(count_result[0]) if count_result else 0
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            conn.close()

    def get_min_max_date(self, table_name: str, column_name: str, schema: Optional[str] = None):
        conn = self.get_connection()
        qualified_table = self._qualify_table_sql(table_name, schema)
        try:
            with conn.cursor() as cur:
                sql = (
                    f"SELECT MIN({self._quote_identifier(column_name)}), MAX({self._quote_identifier(column_name)}) "
                    f"FROM {qualified_table} "
                    f"WHERE {self._quote_identifier(column_name)} IS NOT NULL"
                )
                cur.execute(sql)
                row = cur.fetchone()
                return (row[0], row[1]) if row else (None, None)
        finally:
            conn.close()

    def extract_table_schema(self, table_name, schema: Optional[str] = None):
        conn = self.get_connection()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        schema_name = (target_schema or self.schema).upper()
        try:
            with conn.cursor() as cursor:
                schema_sql = """
                    SELECT 
                        c.column_id AS position,
                        c.column_name AS name,
                        c.data_type,
                        c.data_length AS max_length,
                        CASE WHEN c.nullable = 'N' THEN 'NO' ELSE 'YES' END AS is_nullable,
                        c.data_default AS default_value,
                        CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key,
                        CASE WHEN fk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_foreign_key,
                        CASE WHEN idx.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_index
                    FROM all_tab_columns c
                    LEFT JOIN (
                        SELECT cc.column_name FROM all_constraints con 
                        JOIN all_cons_columns cc ON con.constraint_name = cc.constraint_name 
                        WHERE con.table_name = :table_name AND con.owner = :schema_name AND con.constraint_type = 'P'
                    ) pk ON c.column_name = pk.column_name
                    LEFT JOIN (
                        SELECT cc.column_name FROM all_constraints con 
                        JOIN all_cons_columns cc ON con.constraint_name = cc.constraint_name 
                        WHERE con.table_name = :table_name AND con.owner = :schema_name AND con.constraint_type = 'R'
                    ) fk ON c.column_name = fk.column_name
                    LEFT JOIN (
                        SELECT column_name FROM all_ind_columns 
                        WHERE table_name = :table_name AND index_owner = :schema_name
                    ) idx ON c.column_name = idx.column_name
                    WHERE c.owner = :schema_name AND c.table_name = :table_name
                    ORDER BY c.column_id
                """
                
                # Pass a dictionary instead of a list for named binds
                bind_params = {
                    "schema_name": schema_name,
                    "table_name": pure_table.upper()
                }
                
                cursor.execute(schema_sql, bind_params)
                rows = cursor.fetchall()

                return [
                    {
                        "position": row[0], 
                        "name": row[1], 
                        "type": cast_oracle_to_postgresql_type(row[2].lower()),
                        "length": row[3], 
                        "nullable": row[4],
                        # LOBs/Longs in data_default need to be read carefully, but string casting is safe here
                        "default": str(row[5]).strip() if row[5] else None, 
                        "primary_key": row[6], 
                        "foreign_key": row[7], 
                        "is_index": row[8],
                    }
                    for row in rows
                ]
        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []
        finally:
            conn.close()

    def create_schema_if_missing(self, schema_name: str):
        """Creates a user/schema in Oracle if it doesn't exist."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Oracle treats schemas and users as the same thing
                # Requires high privileges (DBA)
                sql = f"""
                DECLARE
                    userexist integer;
                BEGIN
                    SELECT count(*) INTO userexist FROM dba_users WHERE username='{schema_name.upper()}';
                    IF (userexist = 0) THEN
                        EXECUTE IMMEDIATE 'CREATE USER {schema_name.upper()} IDENTIFIED BY "default_pwd"';
                        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO {schema_name.upper()}';
                    END IF;
                END;
                """
                cursor.execute(sql)
                conn.commit()
                logger.info(f"Schema/User {schema_name} verified/created.")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            conn.rollback()
        finally:
            conn.close()

    def fetch_deltas(self, cursor, primary_keys, log_table: str, since_ts: datetime, batch_size: int = 10_000, schema: Optional[str] = None):
        primary_keys = self.normalize_primary_keys(primary_keys)
        qualified_log_table = self._qualify_table_sql(log_table, schema)
        if not primary_keys:
            logger.error("fetch_deltas requires at least one primary key column.")
            return

        partition_cols = ", ".join(self._quote_identifier(pk) for pk in primary_keys)
        sql = f"""
            SELECT * FROM (
                SELECT t.*, 
                       ROW_NUMBER() OVER(PARTITION BY {partition_cols} ORDER BY "Date_operation" DESC) as rn
                FROM {qualified_log_table} t
                WHERE "Date_operation" > :1
            )
            WHERE rn = 1
            ORDER BY {partition_cols}, "Date_operation" DESC
            OFFSET :2 ROWS FETCH NEXT :3 ROWS ONLY
        """
        offset = 0
        while True:
            cursor.execute(sql, [since_ts, offset, batch_size])
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [desc[0] for desc in cursor.description]
            for row in rows:
                row_dict = dict(zip(col_names, row))
                row_dict.pop('RN', None) # Remove the injected row_number column
                yield row_dict

            offset += batch_size
            

    def get_table_indexes(self, table_name: str, schema: Optional[str] = None) -> list:
        """Returns a list of indexes for the given table."""
        conn = self.get_connection()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        schema_name = (target_schema or self.schema).upper()
        try:
            with conn.cursor() as cursor:
                sql = """
                    SELECT
                        c.index_name,
                        c.column_name,
                        i.uniqueness
                    FROM all_ind_columns c
                    JOIN all_indexes i
                      ON i.owner = c.index_owner
                     AND i.index_name = c.index_name
                    WHERE c.table_name = :table_name
                      AND c.index_owner = :schema_name
                    ORDER BY c.index_name, c.column_position
                """
                cursor.execute(sql, {"table_name": pure_table.upper(), "schema_name": schema_name})
                rows = cursor.fetchall()
                
                # Group columns by index name
                indexes = {}
                for idx_name, col_name, uniqueness in rows:
                    if idx_name not in indexes:
                        indexes[idx_name] = {"columns": [], "unique": uniqueness == "UNIQUE"}
                    indexes[idx_name]["columns"].append(col_name)
                    
                return [
                    {"name": name, "columns": details["columns"], "unique": details["unique"]}
                    for name, details in indexes.items()
                ]
        except Exception as e:
            logger.error(f"Error getting table indexes for {table_name}: {e}")
            return []
        finally:
            conn.close()

    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> bool:
        conn = self.get_connection()
        qualified_table = self._qualify_table_sql(table_name, schema)
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {qualified_table}")
                conn.commit()
                return True
        except Exception as exc:
            conn.rollback()
            logger.error(f"Failed to truncate table {qualified_table}: {exc}")
            return False
        finally:
            conn.close()
