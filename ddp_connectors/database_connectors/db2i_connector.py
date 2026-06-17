#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

from loguru import logger

from .sql_connector import SqlConnector
from .sql_connector_utils import (
    safe_convert_to_string,
    cast_db2i_to_typescript_types,
    cast_db2i_to_postgresql_type,
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

    def _effective_schema(self) -> str:
        """
        The IBM i library to use. On Db2 for i there is no separate "database" the way
        Postgres has one — the host IS the system and the path/`libraries` property sets
        the default library. Accept the library from either `schema` or `database` so it
        works regardless of which UI field the user filled.
        """
        return (self.schema or self.database or "").strip()

    def _qualify(self, table_name: str) -> str:
        """Qualify a bare table name with the configured library if needed."""
        if not table_name:
            return table_name
        lib = self._effective_schema()
        if lib and "." not in table_name:
            return f'"{lib}"."{table_name}"'
        return table_name

    def get_connection(self):
        """Returns a DB-API 2.0 connection backed by the JTOpen JDBC driver."""
        # Lazy import: keeps this module importable (and the factory working for the
        # other connectors) even when jaydebeapi/JPype1 are not installed.
        import jaydebeapi

        # Build the JDBC URL in the same shape DBeaver uses for Db2 for i:
        #   jdbc:as400://<host>;libraries=<lib>;prompt=false
        # The host alone identifies the IBM i system; there is no "/<database>" path
        # segment. The library is passed via the `libraries` property.
        props = ["prompt=false"]
        lib = self._effective_schema()
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
        """Returns True if the connection is successful, False otherwise."""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # Db2 for i has no bare "SELECT 1"; use the SYSIBM.SYSDUMMY1 catalog table.
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            cursor.fetchone()
            logger.info("Database connection is active.")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100) -> List[dict]:
        query = (
            f"SELECT * FROM {self._qualify(table_name)} "
            f"OFFSET {offset} ROWS "
            f"FETCH FIRST {limit} ROWS ONLY"
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

    def fetch_batch(self, cursor, table_name: str, offset: int, limit: int = 100):
        try:
            query = (
                f"SELECT * FROM {self._qualify(table_name)} "
                f"OFFSET {offset} ROWS FETCH FIRST {limit} ROWS ONLY"
            )
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as exc:
            logger.error(f"Error fetching batch from {table_name}: {exc}")
            return []

    def stream_batch(self, cursor, table_name: str, batch_size: int = 10_000):
        """
        Full-sync streaming: sequentially fetch rows without OFFSET.
        Works best for reloads (truncate + reload). Not suitable for resume.
        """
        try:
            cursor.execute(f"SELECT * FROM {self._qualify(table_name)}")

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
        lib = self._effective_schema()
        try:
            if lib:
                cursor.execute(
                    "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                    "WHERE TABLE_TYPE = 'T' AND TABLE_SCHEMA = ?",
                    [lib],
                )
            else:
                cursor.execute(
                    "SELECT TABLE_NAME FROM QSYS2.SYSTABLES "
                    "WHERE TABLE_TYPE = 'T' AND TABLE_SCHEMA NOT LIKE 'Q%'"
                )
            tables = [row[0].strip() for row in cursor.fetchall()]
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
            sql = (
                "SELECT COLUMN_NAME, DATA_TYPE FROM QSYS2.SYSCOLUMNS "
                "WHERE TABLE_NAME = ?"
            )
            params = [table_name]
            lib = self._effective_schema()
            if lib:
                sql += " AND TABLE_SCHEMA = ?"
                params.append(lib)
            sql += " ORDER BY ORDINAL_POSITION"

            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [
                {"name": row[0].strip(), "type": cast_db2i_to_typescript_types(row[1])}
                for row in rows
            ]
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
            cursor.execute(f"SELECT COUNT(*) FROM {self._qualify(table_name)}")
            count_result = cursor.fetchone()
            return int(count_result[0]) if count_result else 0
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            cursor.close()
            connection.close()

    def get_min_max_date(self, table_name: str, column_name: str):
        """
        Returns (min_value, max_value) for a DATE/TIMESTAMP column in Db2 for i.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            sql = (
                f'SELECT MIN("{column_name}") AS min_val, MAX("{column_name}") AS max_val '
                f"FROM {self._qualify(table_name)} "
                f'WHERE "{column_name}" IS NOT NULL'
            )
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cur.close()
            conn.close()

    def extract_table_schema(self, table_name):
        conn = self.get_connection()
        cursor = conn.cursor()
        lib = self._effective_schema()
        try:
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
                    SELECT DISTINCT COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA
                    FROM QSYS2.SYSKEYS
                ) ix ON ix.COLUMN_NAME = c.COLUMN_NAME
                    AND ix.TABLE_NAME = c.TABLE_NAME
                    AND ix.TABLE_SCHEMA = c.TABLE_SCHEMA
                WHERE c.TABLE_NAME = ?
                {schema_filter}
                ORDER BY c.ORDINAL_POSITION
            """
            params = [table_name]
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
        except Exception as e:
            logger.error(f"Error getting database schema: {str(e)}")
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
        # Keep only the latest log row per primary key, like the SQL Server connector.
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
                return s.strip(), tb.strip()
            return self._effective_schema(), t.strip()

        schema_name, pure_table = _split_schema_table(table_name)

        conn = self.get_connection()
        cur = conn.cursor()
        try:
            results: List[Dict[str, Any]] = []

            # 1) Unique / primary key constraints
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

            # 2) Plain (non-constraint) indexes
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
        except Exception as e:
            logger.error(
                f"Error getting indexes for Db2 for i table {schema_name}.{pure_table}: {e}"
            )
            return []
        finally:
            cur.close()
            conn.close()
