from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
from loguru import logger

from .sql_connector import SqlConnector
from .sql_connector_utils import safe_convert_to_string, cast_sqlserver_to_typescript_types, \
    cast_sqlserver_to_postgresql_type, normalize_ui_column_type


class SqlServerConnector(SqlConnector):
    supports_owned_read_cursor = True

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "ODBC Driver 17 for SQL Server"
        self.schema = "dbo"

    def get_connection(self):
        return self._connect_with_retry()

    def _open_connection(self):
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

    def _quote_identifier(self, identifier: str) -> str:
        normalized = self._normalize_identifier(identifier) or ""
        return f"[{normalized.replace(']', ']]')}]"

    def _qualify_table_sql(self, table_name: str, schema: Optional[str] = None) -> str:
        resolved_schema, resolved_table = self.resolve_schema_and_table(table_name, schema)
        if resolved_schema:
            return f"{self._quote_identifier(resolved_schema)}.{self._quote_identifier(resolved_table)}"
        return self._quote_identifier(resolved_table)

    def get_connection_schemas(self) -> List[str]:
        def _read(cursor):
            logger.info(
                f"[sqlserver][schema_discovery] database={self.database} query_path=sys.schemas exclude_system=true"
            )
            cursor.execute(
                """
                SELECT name
                FROM sys.schemas
                WHERE name NOT IN (
                    'sys',
                    'INFORMATION_SCHEMA',
                    'guest',
                    'db_owner',
                    'db_accessadmin',
                    'db_securityadmin',
                    'db_ddladmin',
                    'db_backupoperator',
                    'db_datareader',
                    'db_datawriter',
                    'db_denydatareader',
                    'db_denydatawriter'
                )
                ORDER BY name;
                """
            )
            return [row[0] for row in cursor.fetchall()]

        return self._run_read_with_retry("get_connection_schemas", _read)

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100, filters=None, schema: Optional[str] = None) -> List[dict]:
        def _read(cur):
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = (
                f"SELECT * "
                f"FROM {qualified_table} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS "
                f"FETCH NEXT {limit} ROWS ONLY;"
            )
            logger.info(f"Fetching batch: table={qualified_table}, offset={offset}, limit={limit}")
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cur.fetchall()
            ]

        return self._run_read_with_retry("extract_data_batch", _read)

    def fetch_batch(self, cursor: pyodbc.Cursor, table_name: str, offset: int, batch_size: int = 100, schema: Optional[str] = None, **kwargs):
        def _read(cursor):
            limit = kwargs.get("limit", batch_size)
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = (
                f"SELECT * FROM {qualified_table} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;"
            )
            cursor.execute(query)
            return cursor.fetchall()

        return self._run_read_with_retry("fetch_batch", _read, cursor=cursor)

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000, schema: Optional[str] = None):
        """
        Full-sync streaming: sequentially fetch rows without OFFSET.
        Works best for reloads (truncate + reload). Not suitable for resume.
        """
        def _stream(cursor):
            cursor.arraysize = batch_size
            qualified_table = self._qualify_table_sql(table_name, schema)
            cursor.execute(f"SELECT * FROM {qualified_table};")

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

        yield from self._iterate_read_with_retry("stream_batch", _stream, cursor=cursor)


    def get_connection_tables(self, schema: Optional[str] = None):
        def _read(cursor):
            target_schema = self._normalize_identifier(schema) or self.schema
            sql = """
                    SELECT t.name
                    FROM sys.tables t
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE t.is_ms_shipped = 0
                      AND s.name = ?
                """
            logger.info(
                f"[sqlserver][table_discovery] database={self.database} selected_schema={target_schema} query_path=sys.tables+sys.schemas"
            )
            cursor.execute(sql, target_schema)
            tables = [row.name for row in cursor.fetchall()]
            return tables

        return self._run_read_with_retry("get_connection_tables", _read)

    def get_connection_columns(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
            sql = """
                    SELECT column_name, data_type
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_schema = ?
                      AND table_name   = ?
                    ORDER BY ordinal_position;
              """
            cursor.execute(sql, target_schema, pure_table)
            rows = cursor.fetchall()

            columns = [
                {
                    "name": row.column_name,
                    "type": normalize_ui_column_type(cast_sqlserver_to_typescript_types(row.data_type)),
                    "alias": row.column_name,
                    "classification": "",
                }
                for row in rows
            ]
            return columns

        return self._run_read_with_retry("get_connection_columns", _read)

    def count_table_rows(self, table_name: str, schema: Optional[str] = None, filters=None) -> int:
        def _read(cursor):
            resolved_schema, _ = self.coerce_schema_and_filters(schema, filters)
            qualified_table = self._qualify_table_sql(table_name, resolved_schema)
            count_result = cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}").fetchone()
            total_count = int(count_result[0]) if count_result else 0
            return total_count

        return self._run_read_with_retry("count_table_rows", _read)

    def get_min_max_date(self, table_name: str, column_name: str, schema: Optional[str] = None):
        """
        Returns (min_value, max_value) for a DATE/DATETIME column in SQL Server.
        """
        def _read(cur):
            qualified_table = self._qualify_table_sql(table_name, schema)
            sql = f"""
                SELECT
                    MIN([{column_name}]) AS min_val,
                    MAX([{column_name}]) AS max_val
                FROM {qualified_table}
                WHERE [{column_name}] IS NOT NULL;
            """
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)

        return self._run_read_with_retry("get_min_max_date", _read)

    def extract_table_schema(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            qualified_table = self.qualify_table_name(table_name, schema)
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

            rows = cursor.execute(schema_sql, qualified_table, qualified_table, qualified_table, qualified_table).fetchall()
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

        return self._run_read_with_retry("extract_table_schema", _read)

    def fetch_deltas(
        self,
        cursor,
        primary_keys,
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
        schema: Optional[str] = None,
    ):
        def _stream(cursor):
            keys = self.normalize_primary_keys(primary_keys)
            qualified_log_table = self._qualify_table_sql(log_table, schema)
            if not keys:
                raise ValueError("fetch_deltas requires at least one primary key column.")
            pk_cols = ", ".join(self._quote_identifier(pk) for pk in keys)
            partition_expr = pk_cols                                   # PARTITION BY siren, code
            order_expr = "Date_operation DESC"                         # always the same
            order_by_final = pk_cols
            sql = f"""
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_expr}
                            ORDER BY {order_expr}
                        ) AS rn
                    FROM {qualified_log_table}
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

        yield from self._iterate_read_with_retry("fetch_deltas", _stream, cursor=cursor)

    def get_table_indexes(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
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
        def _read(cur):
            schema_name, pure_table = self.resolve_schema_and_table(table_name, schema)
            schema_name = schema_name or self.schema
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

        return self._run_read_with_retry("get_table_indexes", _read)
