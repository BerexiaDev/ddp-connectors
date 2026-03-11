#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re
import oracledb
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List, Tuple
from .sql_connector import SqlConnector
from .sql_connector_utils import cast_oracle_to_postgresql_type, cast_oracle_to_typescript, safe_convert_to_string

from .sql_connector_utils import cast_oracle_to_postgresql_type, cast_oracle_to_typescript, safe_convert_to_string


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

    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 10_000) -> int:
        """
        Inserts a list of dictionaries into the specified Oracle table using executemany.
        
        Args:
            table_name (str): The target table name.
            data (List[Dict[str, Any]]): The data to insert, where each dictionary is a row.
            batch_size (int): The number of rows to insert per batch to manage memory.
            
        Returns:
            int: The total number of rows successfully inserted.
        """
        if not data:
            logger.warning(f"No data provided to insert into {table_name}.")
            return 0

        # 1. Extract column names from the first dictionary
        # Assumes all dictionaries in the list have the same keys
        columns = list(data[0].keys())
        
        # 2. Build the parameterised SQL statement
        # Double-quote columns to preserve case if needed, and use Oracle bind variables (:1, :2)
        cols_str = ", ".join([f'"{col}"' for col in columns])
        binds_str = ", ".join([f":{i+1}" for i in range(len(columns))])
        
        sql = f'INSERT INTO {self.schema}."{table_name.upper()}" ({cols_str}) VALUES ({binds_str})'
        logger.debug(f"Insert SQL prepared: {sql}")

        # 3. Convert List[Dict] to List[Tuple] matching the column order
        rows_to_insert = [tuple(row.get(col) for col in columns) for row in data]
        
        total_inserted = 0
        conn = self.get_connection()
        
        try:
            with conn.cursor() as cursor:
                # 4. Insert data in chunks using executemany
                for i in range(0, len(rows_to_insert), batch_size):
                    batch = rows_to_insert[i:i + batch_size]
                    
                    # executemany is significantly faster than executing row-by-row
                    cursor.executemany(sql, batch)
                    total_inserted += cursor.rowcount
                    
                # 5. Explicitly commit the transaction
                conn.commit()
                logger.info(f"Successfully inserted {total_inserted} rows into {self.schema}.{table_name}.")
                
                return total_inserted
                
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