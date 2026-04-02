from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

import pyodbc
from loguru import logger

from .sql_connector import SqlConnector
from .sql_connector_utils import safe_convert_to_string, cast_sqlserver_to_typescript_types, \
    cast_sqlserver_to_postgresql_type


class SqlServerConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "ODBC Driver 17 for SQL Server"

    def get_connection(self):
        """Returns a pyodbc connection object directly."""
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "Connection Timeout=10;"
        )
        return pyodbc.connect(conn_str, timeout=10)

    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100) -> List[dict]:
        query = (
            f"SELECT * "
            f"FROM {table_name} "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET {offset} ROWS "
            f"FETCH NEXT {limit} ROWS ONLY;"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cur.fetchall()
            ]
        except Exception as exc:
            logger.error(f"Error extracting batch from {table_name}: {exc}")
            return []
        finally:
            cur.close()
            conn.close()

    def fetch_batch(self, cursor: pyodbc.Cursor, table_name: str, offset: int, limit: int = 100):
        try:
            query = (
                f"SELECT * FROM {table_name} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;"
            )
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as exc:
            logger.error(f"Error fetching batch from {table_name}: {exc}")
            return []

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000, **kwargs):
        """
        Full-sync streaming: sequentially fetch rows without OFFSET.
        Works best for reloads (truncate + reload). Not suitable for resume.
        """
        try:
            cursor.arraysize = batch_size
            cursor.execute(f"SELECT * FROM {table_name};")
    
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
    
        except Exception as exc:
            logger.error(f"Error streaming batch from {table_name}: {exc}")
            return


    def get_connection_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        sql = """
                SELECT  t.name
                FROM sys.tables t
                WHERE t.is_ms_shipped = 0
            """
        try:
            cursor.execute(sql)
            tables = [row.name for row in cursor.fetchall()]
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_connection_columns(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            sql = """
                    SELECT column_name, data_type
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name   = ?;
              """
            cursor.execute(sql, table_name)
            rows = cursor.fetchall()

            columns = [{'name': row.column_name, 'type': cast_sqlserver_to_typescript_types(row.data_type)} for row in
                       rows]
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def count_table_rows(self, table_name: str) -> int:
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            count_result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            cursor.close()
            connection.close()

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE/DATETIME column in SQL Server.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            sql = f"""
                SELECT
                    MIN([{column_name}]) AS min_val,
                    MAX([{column_name}]) AS max_val
                FROM [{table_name}]
                WHERE [{column_name}] IS NOT NULL;
            """
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cur.close()
            conn.close()

    def extract_table_schema(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            schema_sql = """
                    WITH pk_cols AS (
                        SELECT c.name AS col_name
                        FROM sys.indexes i
                        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
                    ),
                    fk_cols AS (
                        SELECT c.name AS col_name
                        FROM sys.foreign_key_columns fkc
                        JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
                        WHERE fkc.parent_object_id = OBJECT_ID(?)
                    ),
                    idx_cols AS (
                        SELECT DISTINCT c.name AS col_name
                        FROM sys.indexes i
                        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 0
                    )
                    SELECT
                        col.column_id,
                        col.name,
                        TYPE_NAME(col.user_type_id) AS data_type,
                        col.max_length,
                        IIF(col.is_nullable = 1, 'YES', 'NO') AS is_nullable,
                        OBJECT_DEFINITION(col.default_object_id) AS default_value,
                        IIF(pk.col_name IS NOT NULL, 'YES', 'NO') AS is_primary_key,
                        IIF(fk.col_name IS NOT NULL, 'YES', 'NO') AS is_foreign_key,
                        IIF(ix.col_name IS NOT NULL, 'YES', 'NO') AS is_indexed
                    FROM sys.columns col
                    LEFT JOIN pk_cols pk ON col.name = pk.col_name
                    LEFT JOIN fk_cols fk ON col.name = fk.col_name
                    LEFT JOIN idx_cols ix ON col.name = ix.col_name
                    WHERE col.object_id = OBJECT_ID(?)
                    ORDER BY col.column_id;
                """

            rows = cursor.execute(schema_sql, table_name, table_name, table_name, table_name).fetchall()
            result = []
            seen = set()
            for row in rows:
                sql_type = row.data_type.upper()

                # Handle any LOB/“MAX” types where max_length = -1
                if row.max_length == -1:

                    if sql_type in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "TEXT", "NTEXT"):
                        pg_type = "TEXT"

                    elif sql_type in ("VARBINARY", "IMAGE"):
                        pg_type = "BYTEA"

                    elif sql_type == "XML":
                        pg_type = "XML"

                    else:
                        pg_type = "TEXT"

                # Otherwise, handle fixed-length or length‐bounded
                else:
                    pg_type = cast_sqlserver_to_postgresql_type(row.data_type)

                # Unique key for dedupe (column name case-insensitive)
                key = row.name.lower()
                if key in seen:
                    logger.warning(f"Duplicate column '{row.name}' in table {table_name}, skipping")
                    continue

                seen.add(key)

                result.append({
                    "position": row.column_id,
                    "name": row.name,
                    "type": pg_type,
                    "length": row.max_length,
                    "nullable": row.is_nullable,
                    "default": row.default_value,
                    "primary_key": row.is_primary_key,
                    "foreign_key": row.is_foreign_key,
                    "is_index": row.is_indexed,
                })

            return result

        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        # Build composite expressions
        pk_cols = ", ".join(primary_keys)                          # ex: "siren, code"
        partition_expr = pk_cols                                   # PARTITION BY siren, code
        order_expr = "Date_operation DESC"                         # always the same
        order_by_final = ", ".join(primary_keys)                   # ORDER BY siren, code

        sql = f"""
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_expr}
                        ORDER BY {order_expr}
                    ) AS rn
                FROM {log_table}
                WHERE Date_operation > ?
            ) AS ranked
            WHERE rn = 1
            ORDER BY {order_by_final}
            OFFSET ? ROWS
            FETCH NEXT ? ROWS ONLY;
        """

        offset = 0
        while True:
            cursor.execute(sql, (since_ts, offset, batch_size))
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [col[0] for col in cursor.description]
            for row in rows:
                yield dict(zip(col_names, row))

            offset += batch_size

    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Return index definitions for a table from SQL Server catalogs.

        Accepts:
        - "agent" (defaults schema to dbo)
        - "dbo.agent" or "schema.table"

        Output example:
        [
        {"name": "IX_agent_sex_datnaiss", "unique": False, "primary": False, "columns": ["sexagent","datnaiss"]},
        {"name": "PK_agent", "unique": True, "primary": True, "columns": ["numaffil"]}
        ]
        """

        def _split_schema_table(t: str) -> Tuple[str, str]:
            t = (t or "").strip()
            # allow [dbo].[agent] style too
            t = t.replace("[", "").replace("]", "")
            if "." in t:
                s, tb = t.split(".", 1)
                return s.strip(), tb.strip()
            return "dbo", t.strip()

        schema_name, pure_table = _split_schema_table(table_name)

        conn = self.get_connection()
        cur = conn.cursor()
        try:
            # Only key columns (exclude included columns); ordered by key_ordinal.
            # Filter out hypothetical/system indexes; keep clustered/nonclustered only.
            sql = """
            SELECT
            i.name AS index_name,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns_csv
            FROM sys.indexes i
            JOIN sys.objects o
            ON o.object_id = i.object_id
            JOIN sys.schemas s
            ON s.schema_id = o.schema_id
            JOIN sys.index_columns ic
            ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE s.name = ?
            AND o.name = ?
            AND o.type = 'U'
            AND i.name IS NOT NULL
            AND i.is_hypothetical = 0
            AND i.type_desc IN ('CLUSTERED', 'NONCLUSTERED')
            AND ic.is_included_column = 0
            AND ic.key_ordinal > 0
            GROUP BY i.name, i.is_unique, i.is_primary_key
            ORDER BY i.is_primary_key DESC, i.is_unique DESC, i.name;
            """

            cur.execute(sql, (schema_name, pure_table))
            rows = cur.fetchall()

            results: List[Dict[str, Any]] = []
            for r in rows:
                idxname = r[0]
                is_unique = bool(r[1])
                is_primary = bool(r[2])
                cols_csv = r[3] or ""
                cols = [c.strip() for c in cols_csv.split(",") if c.strip()]
                if not cols:
                    continue

                results.append(
                    {
                        "name": idxname,
                        "unique": bool(is_unique or is_primary),  # PK implies unique
                        "primary": bool(is_primary),
                        "columns": cols,
                    }
                )

            return results

        except Exception as e:
            logger.error(f"Error getting indexes for SQL Server table {schema_name}.{pure_table}: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def insert_data(self, table_name: str, data: List[Union[Dict[str, Any], Tuple, List]], columns: List[str]) -> int:
        """
        Inserts data into a SQL Server table.
        """
        if not data:
            logger.warning(f"No data provided to insert into {table_name}.")
            return 0

        placeholders = ", ".join(["?"] * len(columns))
        cols_str = ", ".join([f'{col}' for col in columns])
        query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, data)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    
    def upsert_data(self, table_name: str, data: List[Union[Dict[str, Any], Tuple, List]], columns: List[str], pk_columns: List[str]) -> int:
        if not data:
            logger.warning(f"No data provided to upsert into {table_name}.")
            return 0

        if not pk_columns:
            logger.error("Primary key columns must be provided for an upsert operation.")
            raise ValueError("pk_columns cannot be empty for upsert.")

        # 1. Build the USING clause
        # SQL Server allows us to build a virtual source table using VALUES (?, ?, ...)
        placeholders = ", ".join(["?"] * len(columns))
        src_cols = ", ".join([f'[{col}]' for col in columns])
        using_clause = f'USING (VALUES ({placeholders})) AS src({src_cols})'

        # 2. Build the ON clause (defining the match condition)
        on_conditions = " AND ".join([f'trg.[{pk}] = src.[{pk}]' for pk in pk_columns])
        on_clause = f'ON ({on_conditions})'

        # 3. Build the UPDATE clause
        update_cols = [col for col in columns if col not in pk_columns]
        if update_cols:
            set_statements = ", ".join([f'trg.[{col}] = src.[{col}]' for col in update_cols])
            update_clause = f'WHEN MATCHED THEN UPDATE SET {set_statements}'
        else:
            update_clause = ""

        # 4. Build the INSERT clause
        insert_cols_str = ", ".join([f'[{col}]' for col in columns])
        values_str = ", ".join([f'src.[{col}]' for col in columns])
        # IMPORTANT: SQL Server MERGE statements MUST be terminated with a semicolon!
        insert_clause = f'WHEN NOT MATCHED THEN INSERT ({insert_cols_str}) VALUES ({values_str});'

        # 5. Assemble the final MERGE query
        query = f'MERGE {table_name} AS trg \n{using_clause} \n{on_clause} \n{update_clause} \n{insert_clause}'

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, data)
            conn.commit()
            
            upserted_count = cursor.rowcount
            return upserted_count
                
        except Exception as exc:
            conn.rollback()
            logger.error(f"Error upserting data into {table_name}: {exc}")
            raise exc
        finally:
            cursor.close()
            conn.close()