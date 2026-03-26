import oracledb
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_oracle_to_postgresql_type, cast_oracle_to_typescript, safe_convert_to_string

from .sql_connector_utils import cast_oracle_to_postgresql_type, cast_oracle_to_typescript, safe_convert_to_string
from ddp_lib.utils import serialize_if_needed


class OracleConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, schema):
        super().__init__(host, user, password, port, database)
        self.driver = "oracledb"
        self.schema = schema.upper() if schema else user.upper() # Oracle schemas are typically uppercase

    
    def ping(self) -> bool:
        """
        Tests the database connection and verifies the target schema exists.
        Returns True if successful, otherwise False.
        """
        conn = None
        try:
            conn = self.get_connection()

            with conn.cursor() as cursor:
                # Basic ping
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()

                # Check schema exists
                cursor.execute(
                    "SELECT COUNT(*) FROM all_users WHERE username = :schema_name",
                    schema_name=self.schema.upper()
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
            if conn is not None:
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

    def fetch_batch(self, cursor, table_name, offset: int, limit: int = 100):
        try:
            oracledb.defaults.fetch_lobs = False
            query = f'SELECT * FROM {self.schema}.\"{table_name}\" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY'
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

    def stream_batch(self, table_name: str, batch_size: int = 10_000):
        """Streaming for Oracle using fetchmany. Oracle driver handles arraysize natively."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.arraysize = batch_size
                logger.info(f"Start streaming Oracle table {table_name} with batch_size={batch_size}")
                cursor.execute(f"SELECT * FROM {self.schema}.{table_name}")

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    yield rows
                logger.info(f"Finished streaming Oracle table {table_name}")
        except Exception as exc:
            logger.error(f"Error streaming batch from Oracle table {table_name}: {exc}")
        finally:
            conn.close()

    def get_connection_tables(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM all_tables
                    WHERE owner = :1
                    """,
                    [self.schema]
                )
                tables =  [row[0] for row in cur.fetchall()]
                logger.info(f"Tables: {tables}")
                return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            conn.close()

    def get_connection_columns(self, table_name: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM all_tab_columns
                    WHERE owner = :1 AND table_name = :2
                    ORDER BY column_id
                    """,
                    [self.schema, table_name.upper()]
                )
                rows = cur.fetchall()

                columns: list[dict[str, str]] = []
                for column_name, data_type in rows:
                    ts_type = cast_oracle_to_typescript(data_type)
                    columns.append({"name": column_name, "type": ts_type, "alias": column_name})
                return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            conn.close()

    def count_table_rows(self, table_name: str) -> int:
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Add .upper() to the table name
                sql = f"SELECT COUNT(*) FROM {self.schema}.\"{table_name.upper()}\""
                cursor.execute(sql)
                count_result = cursor.fetchone()
                return int(count_result[0]) if count_result else 0
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            conn.close()

    def get_min_max_date(self, table_name: str, column_name: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                sql = (
                    f'SELECT MIN("{column_name}"), MAX("{column_name}") '
                    f'FROM {self.schema}."{table_name}" '
                    f'WHERE "{column_name}" IS NOT NULL'
                )
                cur.execute(sql)
                row = cur.fetchone()
                return (row[0], row[1]) if row else (None, None)
        finally:
            conn.close()

    def extract_table_schema(self, table_name):
        conn = self.get_connection()
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
                    "schema_name": self.schema,
                    "table_name": table_name.upper()
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

    def fetch_deltas(self, cursor, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000):
        # Replaced Postgres DISTINCT ON with ROW_NUMBER window function
        sql = f"""
            SELECT * FROM (
                SELECT t.*, 
                       ROW_NUMBER() OVER(PARTITION BY "{primary_key}" ORDER BY "Date_operation" DESC) as rn
                FROM {self.schema}."{log_table}" t
                WHERE "Date_operation" > :1
            )
            WHERE rn = 1
            ORDER BY "{primary_key}", "Date_operation" DESC
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
            

    def get_table_indexes(self, table_name: str) -> list:
        """Returns a list of indexes for the given table."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                sql = """
                    SELECT index_name, column_name 
                    FROM all_ind_columns 
                    WHERE table_name = :table_name AND index_owner = :schema_name
                    ORDER BY index_name, column_position
                """
                cursor.execute(sql, {"table_name": table_name.upper(), "schema_name": self.schema})
                rows = cursor.fetchall()
                
                # Group columns by index name
                indexes = {}
                for idx_name, col_name in rows:
                    if idx_name not in indexes:
                        indexes[idx_name] = []
                    indexes[idx_name].append(col_name)
                    
                return [{"name": name, "columns": cols} for name, cols in indexes.items()]
        except Exception as e:
            logger.error(f"Error getting table indexes for {table_name}: {e}")
            return []
        finally:
            conn.close()


    def build_create_table_statement(self, table_name: str, schema_name: str = None, columns=None):
        """
        Generates an Oracle CREATE TABLE statement along with CREATE INDEX statements
        (for indexed columns) using the provided column metadata.
        """
        if columns is None:
            columns = []
            
        column_defs = []
        primary_keys = []
        index_keys = []
        
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]
            length = col.get("length")
            nullable = str(col.get("nullable", "")).strip().upper() == "YES"
            is_pk = str(col.get("primary_key", "")).strip().upper() == "YES"
            
            if str(col.get("is_index", "")).strip().upper() == "YES":
                index_keys.append(col_name)

            # Oracle specific string/binary types that need length
            base_type = col_type.split('(')[0].strip().upper()
            if base_type in ("VARCHAR2", "CHAR", "RAW") and length and "(" not in col_type:
                col_type_str = f"{col_type}({length})"
            else:
                col_type_str = col_type

            # Build column definition
            # Quotes ensure case-sensitivity matches the extracted Postgres schema exactly
            col_def_parts = [f'"{col_name}"', col_type_str]

            if not nullable:
                col_def_parts.append("NOT NULL")

            column_defs.append(" ".join(col_def_parts))

            if is_pk:
                primary_keys.append(f'"{col_name}"')

        # Append primary key constraint
        if primary_keys:
            pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
            column_defs.append(pk_def)

        columns_sql = ",\n  ".join(column_defs)
        
        # Postgres often uses 'public', but Oracle uses the schema (user) name.
        # If schema_name is 'public', we omit it so Oracle defaults to the current logged-in user.
        schema_prefix = f'"{schema_name}".' if schema_name and schema_name.lower() != 'public' else ""
        
        # Standard Oracle CREATE TABLE (No IF NOT EXISTS)
        create_stmt = f'CREATE TABLE {schema_prefix}"{table_name}" (\n  {columns_sql}\n);'
        
        index_stmt = None
        if index_keys:
            index_statements = []
            for col in index_keys:
                # Safely slice table and column names for index naming to prevent ORA-00972 (name too long)
                idx_name = f"IDX_{table_name[:12]}_{col[:12]}".upper()
                index_statements.append(
                    f'CREATE INDEX "{idx_name}" ON {schema_prefix}"{table_name}" ("{col}")'
                )
            index_stmt = ";\n".join(index_statements) + ";"

        return create_stmt, index_stmt


    def create_table_if_missing(
        self,
        table_name: str,
        create_table_statement: str,
        index_table_statement: str = None,
    ):
        """Create a table and its indexes in Oracle, ignoring 'already exists' errors."""
        conn = None
        cursor = None

        try:
            if not create_table_statement:
                logger.error("create_table_statement is empty")
                return

            conn = self.get_connection()
            cursor = conn.cursor()

            # Create table
            try:
                clean_create_stmt = create_table_statement.strip().rstrip(";")
                cursor.execute(clean_create_stmt)
                logger.info(f"Table {table_name} created successfully.")
            except Exception as e:
                if "ORA-00955" in str(e):
                    logger.info(f"Table {table_name} already exists. Skipping creation.")
                else:
                    logger.error(f"Failed to create table {table_name}: {e}")
                    raise

            # Create indexes if provided
            if index_table_statement:
                index_stmts = [
                    stmt.strip().rstrip(";")
                    for stmt in index_table_statement.split(";")
                    if stmt.strip()
                ]

                for idx_stmt in index_stmts:
                    try:
                        cursor.execute(idx_stmt)
                        logger.info(f"Index created successfully on {table_name}.")
                    except Exception as e:
                        if "ORA-00955" in str(e) or "ORA-01408" in str(e):
                            logger.info(f"Index on {table_name} already exists. Skipping.")
                        else:
                            logger.error(f"Failed to create index on {table_name}: {e}")
                            raise

        except Exception as e:
            logger.error(f"Error while creating table/indexes for {table_name}: {e}")
            raise

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()
