#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any, List, Dict, Optional
import pyodbc
from loguru import logger
from pyodbc import Cursor

from .sql_connector import SqlConnector, is_parameter_binding_unsupported
from .sql_connector_utils import cast_informix_to_typescript_types, cast_informix_to_postgresql_type, safe_convert_to_string, normalize_ui_column_type


class InformixConnector(SqlConnector):
    supports_owned_read_cursor = True

    def __init__(self, host, user, password, port, database, protocol, locale):
        super().__init__(host, user, password, port, database)
        self.protocol = protocol
        self.locale = locale
        self.driver_path = "app/main/drivers/ddifcl28.so"
        self.schema = user

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            if "first" not in query:
                query = query.replace(";", " ")
                query = f"SELECT FIRST {rows} * FROM ({query})"
        return query

    def get_connection(self):
        return self._connect_with_retry()

    def _open_connection(self):
        conn_str = (
            f"DRIVER={self.driver_path};"
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL={self.protocol};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"CLIENT_LOCALE={self.locale};"   # Explicitly set client locale
            f"DB_LOCALE={self.locale};"       # Explicitly set database locale
            "LoginTimeout=10;"
        )
        conn = pyodbc.connect(conn_str, timeout=10)
        try:
            conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        except BaseException:
            self._safe_close(conn)
            raise
        return conn

    def _qualify_table_sql(self, table_name: str, schema: Optional[str] = None) -> str:
        return self.qualify_table_name(table_name, schema)

    def get_connection_schemas(self) -> List[str]:
        def _read(cursor):
            logger.info(
                f"[informix][schema_discovery] database={self.database} schema_equivalent=owner query_path=systables"
            )
            cursor.execute(
                """
                SELECT DISTINCT owner
                FROM systables
                WHERE tabtype = 'T'
                ORDER BY owner
                """
            )
            return [row[0].strip() for row in cursor.fetchall() if row[0]]

        return self._run_read_with_retry("get_connection_schemas", _read)
    
    
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100, filters=None, schema: Optional[str] = None):
        def _read(cursor):
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = f"SELECT SKIP {offset} FIRST {limit} * FROM {qualified_table};"
            logger.info(f"Fetching batch: table={qualified_table}, offset={offset}, limit={limit}")
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]
            return [
                {col: safe_convert_to_string(row[i]) for i, col in enumerate(column_names)}
                for row in rows
            ]

        return self._run_read_with_retry("extract_data_batch", _read)



    def fetch_batch(self, cursor: Cursor, table_name, offset: int, batch_size: int = 100, schema: Optional[str] = None, **kwargs):
        def _read(cursor):
            limit = kwargs.get("limit", batch_size)
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = f"SELECT SKIP {offset} FIRST {limit} * FROM {qualified_table}"
            cursor.execute(query)
            results = cursor.fetchall()
            return results

        return self._run_read_with_retry("fetch_batch", _read, cursor=cursor)

    def stream_batch(self, cursor: pyodbc.Cursor, table_name: str, batch_size: int = 10_000, schema: Optional[str] = None):
        """
        Full-sync streaming for Informix (no SKIP / OFFSET).
        Uses a single SELECT and fetchmany to avoid performance degradation.
        Suitable for truncate + reload scenarios.
        """
        def _stream(cursor):
            cursor.arraysize = batch_size
            qualified_table = self._qualify_table_sql(table_name, schema)
            logger.info(f"Start streaming Informix table {qualified_table} with batch_size={batch_size}")

            cursor.execute(f"SELECT * FROM {qualified_table}")

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

            logger.info(f"Finished streaming Informix table {qualified_table}")

        yield from self._iterate_read_with_retry("stream_batch", _stream, cursor=cursor)

    def get_connection_tables(self, schema: Optional[str] = None):
        def _read(cursor):
            target_schema = self._normalize_identifier(schema) or self.schema
            if target_schema:
                sql = """
                    SELECT tabname
                    FROM informix.systables
                    WHERE tabtype = 'T'
                      AND tabid >= 100
                      AND owner = ?
                    ORDER BY tabname
                    """
                try:
                    logger.info(
                        f"[informix][table_discovery] database={self.database} selected_schema={target_schema} schema_equivalent=owner query_path=informix.systables parameter_binding=tuple"
                    )
                    cursor.execute(sql, (target_schema,))
                except Exception as error:
                    if not is_parameter_binding_unsupported(error):
                        raise
                    safe_schema = target_schema.replace("'", "''")
                    logger.info(
                        f"[informix][table_discovery] database={self.database} selected_schema={target_schema} schema_equivalent=owner query_path=informix.systables fallback=escaped_literal"
                    )
                    cursor.execute(
                        f"""
                        SELECT tabname
                        FROM informix.systables
                        WHERE tabtype = 'T'
                          AND tabid >= 100
                          AND owner = '{safe_schema}'
                        ORDER BY tabname
                        """
                    )
            else:
                logger.info(
                    f"[informix][table_discovery] database={self.database} selected_schema={target_schema} schema_equivalent=owner query_path=informix.systables unfiltered=true"
                )
                cursor.execute(
                    """
                    SELECT tabname
                    FROM informix.systables
                    WHERE tabtype = 'T'
                      AND tabid >= 100
                    ORDER BY tabname
                    """
                )
            tables = [row[0] for row in cursor.fetchall()]
            return tables

        return self._run_read_with_retry("get_connection_tables", _read)

    def get_connection_columns(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
            query = """
                SELECT c.colname, c.coltype
                FROM informix.syscolumns c
                JOIN informix.systables t ON c.tabid = t.tabid
                WHERE t.tabname = ?
            """
            params: List[Any] = [pure_table]
            if target_schema:
                query += " AND t.owner = ?"
                params.append(target_schema)
            query += " ORDER BY c.colno"

            try:
                logger.debug(
                    f"[informix][column_discovery] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.syscolumns/informix.systables parameter_binding=tuple"
                )
                cursor.execute(query, tuple(params))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                safe_table = pure_table.replace("'", "''")
                if target_schema:
                    safe_schema = target_schema.replace("'", "''")
                    query = f"""
                        SELECT c.colname, c.coltype
                        FROM informix.syscolumns c
                        JOIN informix.systables t ON c.tabid = t.tabid
                        WHERE t.tabname = '{safe_table}'
                          AND t.owner = '{safe_schema}'
                        ORDER BY c.colno
                    """
                else:
                    query = f"""
                        SELECT c.colname, c.coltype
                        FROM informix.syscolumns c
                        JOIN informix.systables t ON c.tabid = t.tabid
                        WHERE t.tabname = '{safe_table}'
                        ORDER BY c.colno
                    """
                logger.debug(
                    f"[informix][column_discovery] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.syscolumns/informix.systables fallback=escaped_literal"
                )
                cursor.execute(query)

            rows = cursor.fetchall()

            columns = [
                {
                    "name": row[0],
                    "type": normalize_ui_column_type(cast_informix_to_typescript_types(row[1])),
                    "alias": row[0],
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
    
    
    def get_database_schema(self) -> Dict[str, Dict]:
        def _read(cursor):
            cursor.execute("""
                SELECT DISTINCT tabname, owner FROM systables
                WHERE tabtype = 'T' AND tabid >= 100
            """)
            tables = {}
            for row in cursor.fetchall():
                table_name = row[0].strip()
                owner = row[1].strip() if row[1] else None
                cursor.execute("""
                    SELECT c.colname, c.coltype, c.collength
                    FROM syscolumns c JOIN systables t ON c.tabid = t.tabid
                    WHERE t.tabname = ? AND t.owner = ? ORDER BY c.colno
                """, (table_name, owner))
                columns = cursor.fetchall()
                if columns:
                    tables[table_name] = {
                        "name": table_name,
                        "owner": owner,
                        "columns": [
                            {"name": col[0].strip(),
                             "type": cast_informix_to_typescript_types(col[1]),
                             "length": col[2]}
                            for col in columns
                        ],
                    }
            return tables

        return self._run_read_with_retry("get_database_schema", _read)

    def extract_table_schema(self, table_name, schema: Optional[str] = None):
        def _read(cursor):
            target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
            query = """
                SELECT
                c.colno      AS ordinal_position,
                c.colname,
                c.coltype,
                c.collength,
                CASE
                    WHEN BITAND(c.coltype, 256) = 256 THEN 'NO'
                    ELSE 'YES'
                END                                       AS is_nullable,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   informix.sysconstraints sc
                        JOIN   informix.sysindexes     si ON sc.idxname = si.idxname
                        WHERE  sc.constrtype = 'P'
                          AND  sc.tabid     = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_primary_key,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   informix.sysconstraints sc
                        JOIN   informix.sysreferences  sr ON sc.constrid = sr.constrid
                        JOIN   informix.sysindexes     si ON sc.idxname  = si.idxname
                        WHERE  sc.constrtype = 'R'
                          AND  sc.tabid      = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_foreign_key,


                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM   informix.sysindexes si
                        WHERE  si.tabid = c.tabid
                          AND  (c.colno = si.part1 OR c.colno = si.part2 OR c.colno = si.part3
                                OR c.colno = si.part4 OR c.colno = si.part5 OR c.colno = si.part6
                                OR c.colno = si.part7 OR c.colno = si.part8 OR c.colno = si.part9
                                OR c.colno = si.part10 OR c.colno = si.part11 OR c.colno = si.part12
                                OR c.colno = si.part13 OR c.colno = si.part14 OR c.colno = si.part15
                                OR c.colno = si.part16)
                    ) THEN 'YES' ELSE 'NO'
                END                                       AS is_index,


                d.default                                 AS default_value
            FROM   informix.syscolumns   c
            JOIN   informix.systables    t ON c.tabid = t.tabid
            LEFT   JOIN informix.sysdefaults d ON c.tabid = d.tabid AND c.colno = d.colno
            WHERE  t.tabname = ?
            """
            params: List[Any] = [pure_table]
            if target_schema:
                query += " AND t.owner = ?"
                params.append(target_schema)
            query += " ORDER BY c.colno"

            try:
                logger.debug(
                    f"[informix][extract_table_schema] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix catalog tables parameter_binding=tuple"
                )
                cursor.execute(query, tuple(params))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                safe_table = pure_table.replace("'", "''")
                fallback_query = query.replace("?", f"'{safe_table}'", 1)
                if target_schema:
                    safe_owner = target_schema.replace("'", "''")
                    fallback_query = fallback_query.replace("?", f"'{safe_owner}'", 1)
                logger.debug(
                    f"[informix][extract_table_schema] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix catalog tables fallback=escaped_literal"
                )
                cursor.execute(fallback_query)

            rows = cursor.fetchall()

            columns = [
                {
                    'position': col[0],
                    'name': col[1],
                    'type': cast_informix_to_postgresql_type(col[2]),
                    'length': col[3],
                    'nullable': col[4],
                    'primary_key': col[5],
                    'foreign_key': col[6],
                    "is_index": col[7],
                    'default': col[8]
                }
                for col in rows
            ]
            return columns

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
            pk_match = " AND ".join(f"lt2.{pk} = lt1.{pk}" for pk in keys)
            order_by = ", ".join(keys)
            sql = f"""
                SELECT SKIP ? FIRST ? *
                FROM {qualified_log_table} lt1
                WHERE lt1.Date_operation = (
                    SELECT MAX(lt2.Date_operation)
                    FROM {qualified_log_table} lt2
                    WHERE {pk_match}
                    AND lt2.Date_operation > ?
                )
                AND lt1.Date_operation > ?
                ORDER BY {order_by};
            """
            offset = 0
            while True:
                cursor.execute(sql, (offset, batch_size, since_ts, since_ts))
                rows = cursor.fetchall()
                if not rows:
                    break

                col_names = [c[0] for c in cursor.description]
                for tup in rows:
                    yield dict(zip(col_names, tup))

                offset += batch_size

        yield from self._iterate_read_with_retry("fetch_deltas", _stream, cursor=cursor)


    def get_min_max_date(self, table_name: str, column_name: str, schema: Optional[str] = None):
        """
        Returns (min_value, max_value) for a DATE / DATETIME column in an Informix table.
        Assumes snake_case identifiers (no spaces), so no quoting.
        """
        def _read(cursor):
            qualified_table = self._qualify_table_sql(table_name, schema)
            sql = f"""
                SELECT MIN({column_name}) AS min_val, MAX({column_name}) AS max_val
                FROM {qualified_table}
                WHERE {column_name} IS NOT NULL
            """
            logger.info(f"Getting min/max for {qualified_table}.{column_name}")
            cursor.execute(sql)
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (None, None)

        return self._run_read_with_retry("get_min_max_date", _read)

    def get_table_indexes(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        def _read(cursor):
            target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)

            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix catalogs start=true"
            )

            # 1) Get tabid once
            sql_tabid = """
                SELECT tabid
                FROM informix.systables
                WHERE tabtype = 'T'
                  AND tabname = ?
            """
            params: List[Any] = [pure_table]
            if target_schema:
                sql_tabid += " AND owner = ?"
                params.append(target_schema)
            try:
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.systables parameter_binding=tuple"
                )
                cursor.execute(sql_tabid, tuple(params))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                safe_table = pure_table.replace("'", "''")
                fallback_query = sql_tabid.replace("?", f"'{safe_table}'", 1)
                if target_schema:
                    safe_owner = target_schema.replace("'", "''")
                    fallback_query = fallback_query.replace("?", f"'{safe_owner}'", 1)
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.systables fallback=escaped_literal"
                )
                cursor.execute(fallback_query)
            row = cursor.fetchone()
            if not row:
                logger.warning(f"Table not found in Informix catalogs: {table_name}")
                return []
            tabid = int(row[0])
            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} tabid={tabid}"
            )

            sql_idx = """
                SELECT
                    idxname, idxtype,
                    part1, part2, part3, part4, part5, part6, part7, part8,
                    part9, part10, part11, part12, part13, part14, part15, part16
                FROM informix.sysindexes
                WHERE tabid = ?
            """
            try:
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysindexes parameter_binding=tuple tabid={tabid}"
                )
                cursor.execute(sql_idx, (tabid,))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysindexes fallback=int_literal tabid={tabid}"
                )
                cursor.execute(sql_idx.replace("?", str(tabid), 1))
            index_rows = cursor.fetchall()
            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysindexes rows={len(index_rows)} tabid={tabid}"
            )
            if not index_rows:
                return []

            sql_columns = "SELECT colno, colname FROM informix.syscolumns WHERE tabid = ?"
            try:
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.syscolumns parameter_binding=tuple tabid={tabid}"
                )
                cursor.execute(sql_columns, (tabid,))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.syscolumns fallback=int_literal tabid={tabid}"
                )
                cursor.execute(sql_columns.replace("?", str(tabid), 1))
            col_map = {int(r[0]): r[1].strip() for r in cursor.fetchall()}
            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.syscolumns mapped_cols={len(col_map)} tabid={tabid}"
            )

            sql_constraints = """
                SELECT constrtype, idxname
                FROM informix.sysconstraints
                WHERE tabid = ?
                AND constrtype IN ('P', 'U')
                """
            try:
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysconstraints parameter_binding=tuple tabid={tabid}"
                )
                cursor.execute(sql_constraints, (tabid,))
            except Exception as error:
                if not is_parameter_binding_unsupported(error):
                    raise
                logger.debug(
                    f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysconstraints fallback=int_literal tabid={tabid}"
                )
                cursor.execute(sql_constraints.replace("?", str(tabid), 1))
            constraint_map: Dict[str, str] = {}
            rows = cursor.fetchall()
            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} query_path=informix.sysconstraints rows={len(rows)} tabid={tabid}"
            )
            for r in rows:
                constrtype = r[0].strip() if r[0] else None
                idxname = r[1].strip() if r[1] else None
                if constrtype and idxname:
                    constraint_map[idxname] = constrtype

            results: List[Dict[str, Any]] = []
            for r in index_rows:
                idxname = (r[0] or "").strip()
                idxtype = (r[1] or "").strip()

                parts = [p for p in r[2:] if p is not None and int(p) > 0]
                cols: List[str] = []
                for colno in parts:
                    colname = col_map.get(int(colno))
                    if colname:
                        cols.append(colname)

                if not cols:
                    continue

                constrtype = constraint_map.get(idxname)
                is_primary = constrtype == "P"
                is_unique = is_primary or (constrtype == "U")

                results.append(
                    {
                        "name": idxname,
                        "unique": bool(is_unique),
                        "primary": bool(is_primary),
                        "columns": cols,
                        "source_idxtype": idxtype,
                        }
                )

            logger.info(
                f"[informix][get_table_indexes] database={self.database} selected_schema={target_schema} table={pure_table} returning={len(results)}"
            )
            return results

        return self._run_read_with_retry("get_table_indexes", _read)
