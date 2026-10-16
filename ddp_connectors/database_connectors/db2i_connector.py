#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from .sql_connector import SqlConnector
from .connection_errors import jdbc_connection_error
from .sql_connector_utils import (
    safe_convert_to_string,
    cast_db2i_to_typescript_types,
    cast_db2i_to_postgresql_type,
    normalize_ui_column_type,
)

# JDBC driver class shipped inside jt400.jar (the open-source JTOpen / IBM Toolbox
# for Java). Freely downloadable from Maven Central / SourceForge and NOT subject to
# the US export controls that gate the IBM i Access ODBC Driver.
_JT400_DRIVER_CLASS = "com.ibm.as400.access.AS400JDBCDriver"


class Db2iConnector(SqlConnector):
    """
    Connector for Db2 for IBM i (a.k.a. Db2/400, the database on IBM i / AS400).

    Uses the open-source JTOpen JDBC driver (jt400.jar) through jaydebeapi instead of
    ODBC, so no export-controlled IBM binary is required and the JVM already present in
    the service images is reused. The Db2-for-i "schema" is an IBM i *library*; pass it
    via the optional `schema` setting. Catalog metadata is read from the QSYS2 system
    catalog views (SYSTABLES, SYSCOLUMNS, ...).

    The path to jt400.jar is taken from the JT400_JAR environment variable (set in the
    service Dockerfiles), falling back to a vendored location under app/main/drivers/.
    """

    supports_owned_read_cursor = True
    _is_retryable_connection_error = staticmethod(jdbc_connection_error)

    def __init__(self, host, user, password, port, database, schema=None):
        super().__init__(host, user, password, port, database)
        self.driver = "jt400"
        # On Db2 for i the "schema" is a library (collection). Optional.
        self.schema = schema

    # ------------------------------------------------------------------ helpers
    def _jt400_jar_path(self) -> str:
        return os.environ.get(
            "JT400_JAR", "/usr/src/app/app/main/drivers/jt400.jar"
        )

    def _lib(self, override: Optional[str] = None) -> str:
        """
        The IBM i library to use, upper-cased. On Db2 for i there is no separate
        "database" the way Postgres has one — the host IS the system and the
        `libraries` property sets the default library. Accept the library from the
        explicit override (e.g. the UI schema selector), else `schema`, else
        `database`, so it works regardless of which field the user filled.

        Library names on IBM i are stored upper-case in the QSYS2 catalog, and the
        API lower-cases `database` on save, so we upper-case here to match reliably.
        """
        return (override or self.schema or self.database or "").strip().upper()

    def _split_schema_table(self, table_name: str, schema: Optional[str] = None) -> Tuple[str, str]:
        """
        Resolve (library, table) from a possibly-qualified name. Accepts "LIB.TABLE"
        or a bare "TABLE" (with the library taken from the schema arg / config). Both
        parts are upper-cased to match the QSYS2 catalog (IBM i stores system/DDS
        names upper-case, and the API lower-cases identifiers on save).
        """
        raw = (table_name or "").strip().replace('"', "")
        if "." in raw:
            lib, pure = raw.split(".", 1)
            return lib.strip().upper(), pure.strip().upper()
        return self._lib(schema), raw.upper()

    def _qualify(self, table_name: str, schema: Optional[str] = None) -> str:
        """Qualify a table name as "LIB"."TABLE", upper-casing both parts for IBM i."""
        if not table_name:
            return table_name
        lib, pure = self._split_schema_table(table_name, schema)
        if lib:
            return f'"{lib}"."{pure}"'
        return f'"{pure}"'

    def get_connection(self):
        return self._connect_with_retry()

    def _open_connection(self):
        """Returns a DB-API 2.0 connection backed by the JTOpen JDBC driver."""
        # Lazy import: keeps this module importable (and the factory working for the
        # other connectors) even when jaydebeapi/JPype1 are not installed.
        import jaydebeapi

        # Build the JDBC URL in the same shape DBeaver uses for Db2 for i:
        #   jdbc:as400://<host>;libraries=<lib>;prompt=false
        # The host alone identifies the IBM i system; there is no "/<database>" path
        # segment. The library is passed via the `libraries` property.
        props = ["prompt=false"]
        lib = self._lib()
        if lib:
            props.append(f"libraries={lib}")
        if self.port:
            # Optional: override the default DRDA port (446) only if one is given.
            props.append(f"portNumber={self.port}")

        url = f"jdbc:as400://{self.host};" + ";".join(props)

        return jaydebeapi.connect(
            _JT400_DRIVER_CLASS,
            url,
            [self.user, self.password],
            self._jt400_jar_path(),
        )

    def ping(self):
        def _ping(cursor):
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            cursor.fetchone()
            return True

        try:
            return self._run_read_with_retry("ping", _ping)
        except Exception as error:
            logger.error(f"Database connection failed: {error}")
            return False

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100) -> List[dict]:
        query = (
            f"SELECT * FROM {self._qualify(table_name)} "
            f"OFFSET {offset} ROWS "
            f"FETCH FIRST {limit} ROWS ONLY"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}")
        def _read(cur):
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cur.fetchall()
            ]

        return self._run_read_with_retry("extract_data_batch", _read)

    def fetch_batch(self, cursor, table_name: str, offset: int, limit: int = 100):
        def _read(cursor):
            query = (
                f"SELECT * FROM {self._qualify(table_name)} "
                f"OFFSET {offset} ROWS FETCH FIRST {limit} ROWS ONLY"
            )
            cursor.execute(query)
            return cursor.fetchall()

        return self._run_read_with_retry("fetch_batch", _read, cursor=cursor)

    def stream_batch(self, cursor, table_name: str, batch_size: int = 10_000):
        """Stream on owned or borrowed handles; reconnect only before yielding rows."""
        qualified_table = self._qualify(table_name)
        def _stream(active_cursor):
            active_cursor.execute(f"SELECT * FROM {qualified_table}")
            while True:
                rows = active_cursor.fetchmany(batch_size)
                if not rows:
                    return
                yield rows

        yield from self._iterate_read_with_retry("stream_batch", _stream, cursor=cursor)

    def get_connection_schemas(self) -> List[str]:
        """
        List the IBM i libraries (schemas) available, excluding the system libraries
        (the Q* / SYS* collections). The configured library is reordered to the front
        by the API layer.
        """
        def _read(cursor):
            cursor.execute(
                "SELECT SCHEMA_NAME FROM QSYS2.SYSSCHEMAS "
                "WHERE SCHEMA_NAME NOT LIKE 'Q%' AND SCHEMA_NAME NOT LIKE 'SYS%' "
                "ORDER BY SCHEMA_NAME"
            )
            return [row[0].strip() for row in cursor.fetchall() if row[0]]

        return self._run_read_with_retry("get_connection_schemas", _read)

    def get_connection_tables(self, schema: Optional[str] = None):
        def _read(cursor):
            lib = self._lib(schema)
            if lib:
                cursor.execute(
                    "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                    "WHERE TABLE_TYPE IN ('T', 'P') AND TABLE_SCHEMA = ? "
                    "ORDER BY TABLE_NAME",
                    [lib],
                )
            else:
                cursor.execute(
                    "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                    "WHERE TABLE_TYPE IN ('T', 'P') AND TABLE_SCHEMA NOT LIKE 'Q%' "
                    "ORDER BY TABLE_NAME"
                )
            tables = [row[0].strip() for row in cursor.fetchall()]
            return tables

        return self._run_read_with_retry("get_connection_tables", _read)

    def get_connection_columns(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            lib, pure_table = self._split_schema_table(table_name, schema)
            sql = (
                "SELECT COLUMN_NAME, DATA_TYPE FROM QSYS2.SYSCOLUMNS "
                "WHERE TABLE_NAME = ?"
            )
            params = [pure_table]
            if lib:
                sql += " AND TABLE_SCHEMA = ?"
                params.append(lib)
            sql += " ORDER BY ORDINAL_POSITION"
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [
                {
                    "name": row[0].strip(),
                    "type": normalize_ui_column_type(cast_db2i_to_typescript_types(row[1])),
                    "alias": row[0].strip(),
                    "classification": "",
                }
                for row in rows
            ]
            return columns

        return self._run_read_with_retry("get_connection_columns", _read)

    def count_table_rows(self, table_name: str) -> int:
        def _read(cursor):
            cursor.execute(f"SELECT COUNT(*) FROM {self._qualify(table_name)}")
            count_result = cursor.fetchone()
            return int(count_result[0]) if count_result else 0

        return self._run_read_with_retry("count_table_rows", _read)

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE/TIMESTAMP column in Db2 for i.
        """
        def _read(cur):
            sql = (
                f'SELECT MIN("{column_name}") AS min_val, MAX("{column_name}") AS max_val '
                f"FROM {self._qualify(table_name)} "
                f'WHERE "{column_name}" IS NOT NULL'
            )
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)

        return self._run_read_with_retry("get_min_max_date", _read)

    def extract_table_schema(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            lib, pure_table = self._split_schema_table(table_name, schema)
            schema_filter = "AND c.TABLE_SCHEMA = ?" if lib else ""
            sql = f"""
                SELECT
                    c.ORDINAL_POSITION,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.LENGTH,
                    c.IS_NULLABLE,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY,
                    CASE WHEN fk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_FOREIGN_KEY,
                    CASE WHEN ix.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_INDEX,
                    c.COLUMN_DEFAULT
                FROM QSYS2.SYSCOLUMNS c
                LEFT JOIN (
                    SELECT k.COLUMN_NAME, k.TABLE_NAME, k.TABLE_SCHEMA
                    FROM QSYS2.SYSKEYCST k
                    JOIN QSYS2.SYSCST t
                      ON k.CONSTRAINT_NAME = t.CONSTRAINT_NAME
                     AND k.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
                    WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
                    AND pk.TABLE_NAME = c.TABLE_NAME
                    AND pk.TABLE_SCHEMA = c.TABLE_SCHEMA
                LEFT JOIN (
                    SELECT k.COLUMN_NAME, k.TABLE_NAME, k.TABLE_SCHEMA
                    FROM QSYS2.SYSKEYCST k
                    JOIN QSYS2.SYSCST t
                      ON k.CONSTRAINT_NAME = t.CONSTRAINT_NAME
                     AND k.CONSTRAINT_SCHEMA = t.CONSTRAINT_SCHEMA
                    WHERE t.CONSTRAINT_TYPE = 'FOREIGN KEY'
                ) fk ON fk.COLUMN_NAME = c.COLUMN_NAME
                    AND fk.TABLE_NAME = c.TABLE_NAME
                    AND fk.TABLE_SCHEMA = c.TABLE_SCHEMA
                LEFT JOIN (
                    SELECT DISTINCT k.COLUMN_NAME, i.TABLE_NAME, i.TABLE_SCHEMA
                    FROM QSYS2.SYSKEYS k
                    JOIN QSYS2.SYSINDEXES i
                      ON k.INDEX_NAME = i.INDEX_NAME
                     AND k.INDEX_SCHEMA = i.INDEX_SCHEMA
                ) ix ON ix.COLUMN_NAME = c.COLUMN_NAME
                    AND ix.TABLE_NAME = c.TABLE_NAME
                    AND ix.TABLE_SCHEMA = c.TABLE_SCHEMA
                WHERE c.TABLE_NAME = ?
                {schema_filter}
                ORDER BY c.ORDINAL_POSITION
            """
            params = [pure_table]
            if lib:
                params.append(lib)
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [
                {
                    "position": col[0],
                    "name": col[1].strip() if col[1] else col[1],
                    "type": cast_db2i_to_postgresql_type(col[2]),
                    "length": col[3],
                    "nullable": col[4],
                    "primary_key": col[5],
                    "foreign_key": col[6],
                    "is_index": col[7],
                    "default": col[8],
                }
                for col in rows
            ]
            return columns

        return self._run_read_with_retry("extract_table_schema", _read)

    def fetch_deltas(
        self,
        cursor,
        primary_keys: List[str],
        log_table: str,
        since_ts: datetime,
        batch_size: int = 10_000,
    ):
        # Keep only the latest log row per primary key, like the SQL Server connector.
        def _stream(cursor):
            partition_expr = ", ".join(primary_keys)
            order_by_final = ", ".join(primary_keys)
            sql = f"""
                SELECT *
                FROM (
                    SELECT t.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY {partition_expr}
                            ORDER BY Date_operation DESC
                        ) AS rn
                    FROM {self._qualify(log_table)} t
                    WHERE Date_operation > ?
                ) AS ranked
                WHERE rn = 1
                ORDER BY {order_by_final}
                OFFSET ? ROWS
                FETCH FIRST ? ROWS ONLY
            """
            offset = 0
            while True:
                cursor.execute(sql, [since_ts, offset, batch_size])
                rows = cursor.fetchall()
                if not rows:
                    break

                col_names = [col[0] for col in cursor.description]
                for row in rows:
                    yield dict(zip(col_names, row))

                offset += batch_size

        yield from self._iterate_read_with_retry("fetch_deltas", _stream, cursor=cursor)

    def truncate_table(self, table_name: str) -> bool:
        """Remove all data from the table while keeping its structure."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"TRUNCATE TABLE {self._qualify(table_name)} IMMEDIATE"
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Return index/key definitions for a Db2-for-i table from the QSYS2 catalog.

        Accepts "table" or "schema.table". Unique/primary-key constraints come from
        SYSCST + SYSKEYCST; plain indexes come from SYSINDEXES + SYSKEYS.
        """
        def _split_schema_table(t: str) -> Tuple[str, str]:
            t = (t or "").strip().replace('"', "")
            if "." in t:
                s, tb = t.split(".", 1)
                return s.strip().upper(), tb.strip()
            return self._lib(), t.strip()
        schema_name, pure_table = _split_schema_table(table_name)
        def _read(cur):
            results: List[Dict[str, Any]] = []
            cst_sql = """
                SELECT t.CONSTRAINT_NAME, t.CONSTRAINT_TYPE, k.COLUMN_NAME, k.ORDINAL_POSITION
                FROM QSYS2.SYSCST t
                JOIN QSYS2.SYSKEYCST k
                  ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                 AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                WHERE t.TABLE_NAME = ?
                  AND t.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
            """
            params = [pure_table]
            if schema_name:
                cst_sql += " AND t.TABLE_SCHEMA = ?"
                params.append(schema_name)
            cst_sql += " ORDER BY t.CONSTRAINT_NAME, k.ORDINAL_POSITION"
            cur.execute(cst_sql, params)
            cst_map: Dict[str, Dict[str, Any]] = {}
            for r in cur.fetchall():
                cname = (r[0] or "").strip()
                ctype = (r[1] or "").strip()
                col = (r[2] or "").strip()
                entry = cst_map.setdefault(
                    cname,
                    {
                        "name": cname,
                        "unique": True,
                        "primary": ctype == "PRIMARY KEY",
                        "columns": [],
                    },
                )
                if col:
                    entry["columns"].append(col)
            results.extend(v for v in cst_map.values() if v["columns"])
            idx_sql = """
                SELECT i.INDEX_NAME, i.IS_UNIQUE, k.COLUMN_NAME, k.ORDINAL_POSITION
                FROM QSYS2.SYSINDEXES i
                JOIN QSYS2.SYSKEYS k
                  ON i.INDEX_NAME = k.INDEX_NAME
                 AND i.INDEX_SCHEMA = k.INDEX_SCHEMA
                WHERE i.TABLE_NAME = ?
            """
            params = [pure_table]
            if schema_name:
                idx_sql += " AND i.TABLE_SCHEMA = ?"
                params.append(schema_name)
            idx_sql += " ORDER BY i.INDEX_NAME, k.ORDINAL_POSITION"
            cur.execute(idx_sql, params)
            idx_map: Dict[str, Dict[str, Any]] = {}
            for r in cur.fetchall():
                iname = (r[0] or "").strip()
                is_unique = str(r[1]).strip().upper() in ("Y", "YES", "1", "U")
                col = (r[2] or "").strip()
                if iname in cst_map:
                    continue  # already reported as a constraint
                entry = idx_map.setdefault(
                    iname,
                    {"name": iname, "unique": is_unique, "primary": False, "columns": []},
                )
                if col:
                    entry["columns"].append(col)
            results.extend(v for v in idx_map.values() if v["columns"])
            return results

        return self._run_read_with_retry("get_table_indexes", _read)
