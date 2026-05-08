#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re
import mysql.connector
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List, Optional, Tuple

from .sql_connector import SqlConnector
from .sql_connector_utils import (
    cast_mysql_to_typescript_types,
    cast_mysql_to_postgresql_type,
    normalize_ui_column_type,
    safe_convert_to_string,
)


class MySQLConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "mysql+mysqlconnector"

    def get_connection(self):
        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "port": int(self.port),
            "database": self.database,
            "connection_timeout": 10,
            "use_pure": True,
        }
        return mysql.connector.connect(**conn_params)

    def get_default_schema(self) -> Optional[str]:
        return self.database

    def _quote_identifier(self, identifier: str) -> str:
        normalized = self._normalize_identifier(identifier) or ""
        return f"`{normalized.replace('`', '``')}`"

    def _qualify_table_sql(self, table_name: str, schema: Optional[str] = None) -> str:
        resolved_schema, resolved_table = self.resolve_schema_and_table(table_name, schema)
        if resolved_schema:
            return f"{self._quote_identifier(resolved_schema)}.{self._quote_identifier(resolved_table)}"
        return self._quote_identifier(resolved_table)

    def get_connection_schemas(self) -> List[str]:
        return [self.database]

    def _build_filters_clause(self, filters) -> Tuple[str, List[Any]]:
        """Parse filters payload into a safe WHERE clause and parameters."""
        parsed_filters = []
        if isinstance(filters, str):
            filters_str = filters.strip()
            if filters_str:
                try:
                    parsed_filters = json.loads(filters_str)
                    if not isinstance(parsed_filters, list):
                        logger.error("Filters must be a JSON array; ignoring filters.")
                        parsed_filters = []
                except json.JSONDecodeError as exc:
                    logger.error(f"Invalid filters JSON provided; ignoring filters. {exc}")
            else:
                logger.warning("Empty filters string provided; ignoring filters.")
        elif isinstance(filters, list):
            parsed_filters = filters
        elif filters is not None:
            logger.error("Filters must be a JSON string or list; ignoring filters.")

        clauses: List[str] = []
        params: List[Any] = []
        if parsed_filters:
            op_map = {
                "CONTAINS": ("LIKE", lambda v: f"%{v}%"),
                "NOT_CONTAINS": ("NOT LIKE", lambda v: f"%{v}%"),
                "NOT_CONTAIN": ("NOT LIKE", lambda v: f"%{v}%"),
                "STARTS_WITH": ("LIKE", lambda v: f"{v}%"),
                "ENDS_WITH": ("LIKE", lambda v: f"%{v}"),
                "MATCHES": ("REGEXP", lambda v: v),
                "NOT_MATCHES": ("NOT REGEXP", lambda v: v),
                "=": "=",
                "EQUALS": "=",
                "!=": "!=",
                "NOT_EQUALS": "!=",
                ">": ">",
                "GREATER_THAN": ">",
                "<": "<",
                "LESS_THAN": "<",
                ">=": ">=",
                "GREATER_THAN_OR_EQUAL": ">=",
                "<=": "<=",
                "LESS_THAN_OR_EQUAL": "<=",
                "BETWEEN": "BETWEEN",
                "NOT_BETWEEN": "NOT BETWEEN",
                "IN": "IN",
                "NOT_IN": "NOT IN",
                "IS_NULL": "IS NULL",
                "IS_NOT_NULL": "IS NOT NULL",
            }

            for condition in parsed_filters:
                col_info = condition.get("column") or {}
                col_name = col_info.get("name")
                if not col_name or not isinstance(col_name, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col_name):
                    logger.warning(f"Skipping filter with invalid column name: {col_name}")
                    continue

                raw_operator = condition.get("operator")
                op_key = str(raw_operator).strip().upper().replace(" ", "_") if raw_operator else None
                sql_op = op_map.get(op_key) if op_key else None
                if not sql_op:
                    logger.warning(f"Skipping unsupported operator '{raw_operator}' for column '{col_name}'")
                    continue

                value = condition.get("value")
                value_to = condition.get("valueTo")

                if op_key in ("BETWEEN", "NOT_BETWEEN"):
                    if value is None or value_to is None:
                        logger.warning(f"Skipping BETWEEN filter for '{col_name}' because bounds are missing.")
                        continue
                    clauses.append(f"`{col_name}` {sql_op} %s AND %s")
                    params.extend([value, value_to])
                elif op_key in ("IN", "NOT_IN"):
                    values = value
                    if isinstance(values, str):
                        values = [v.strip() for v in values.split(",") if v.strip()]
                    if not isinstance(values, list) or not values:
                        logger.warning(f"Skipping IN filter for '{col_name}' due to empty values.")
                        continue
                    placeholders = ", ".join(["%s"] * len(values))
                    clauses.append(f"`{col_name}` {sql_op} ({placeholders})")
                    params.extend(values)
                elif op_key in ("IS_NULL", "IS_NOT_NULL"):
                    clauses.append(f"`{col_name}` {sql_op}")
                elif isinstance(sql_op, tuple):
                    sql_operator, pattern_builder = sql_op
                    if value is None:
                        logger.warning(f"Skipping filter for '{col_name}' because value is missing.")
                        continue
                    clauses.append(f"`{col_name}` {sql_operator} %s")
                    params.append(pattern_builder(value))
                else:
                    if value is None:
                        logger.warning(f"Skipping filter for '{col_name}' because value is missing.")
                        continue
                    clauses.append(f"`{col_name}` {sql_op} %s")
                    params.append(value)

        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_clause, params

    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100, filters=None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        where_clause, params = self._build_filters_clause(filters)
        qualified_table = self._qualify_table_sql(table_name, schema)
        query = (
            f"SELECT * FROM {qualified_table}"
            f"{where_clause} "
            f"LIMIT {limit} OFFSET {offset};"
        )
        logger.info(f"Fetching batch: table={qualified_table}, offset={offset}, limit={limit}, filters_applied={bool(where_clause.strip())}")
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            cols = [c[0] for c in cursor.description]
            return [
                {col: safe_convert_to_string(row[idx]) for idx, col in enumerate(cols)}
                for row in cursor.fetchall()
            ]
        except Exception as exc:
            logger.error(f"Error extracting batch from {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_batch(self, cursor, table_name, offset: int, batch_size: int = 100, schema: Optional[str] = None, **kwargs):
        try:
            limit = kwargs.get("limit", batch_size)
            qualified_table = self._qualify_table_sql(table_name, schema)
            query = f"SELECT * FROM {qualified_table} LIMIT {limit} OFFSET {offset}"
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []

    def stream_batch(self, cursor=None, table_name: Optional[str] = None, batch_size: int = 10_000, schema: Optional[str] = None):
        """
        Full-sync streaming for MySQL using a server-side cursor (SSCursor).
        Avoids OFFSET and prevents loading the full result set into memory.
        """
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
                managed_cursor = conn.cursor(buffered=False)
            else:
                managed_cursor = cursor

            logger.info(f"Start streaming MySQL table {qualified_table} with batch_size={batch_size}")
            managed_cursor.execute(f"SELECT * FROM {qualified_table}")

            while True:
                rows = managed_cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

            logger.info(f"Finished streaming MySQL table {qualified_table}")

        except Exception as exc:
            logger.error(f"Error streaming batch from MySQL table {table_name}: {exc}")
            return

        finally:
            if managed_cursor and conn:
                managed_cursor.close()
            if conn:
                conn.close()

    def get_connection_tables(self, schema: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        target_schema = self._normalize_identifier(schema) or self.database
        try:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE';
                """,
                (target_schema,),
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_connection_columns(self, table_name: str, schema: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        try:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position;
                """,
                (target_schema, pure_table),
            )
            rows = cursor.fetchall()

            columns: List[Dict[str, str]] = []
            for column_name, data_type in rows:
                ts_type = normalize_ui_column_type(cast_mysql_to_typescript_types(data_type))
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
            cursor.close()
            conn.close()

    def count_table_rows(self, table_name: str, schema: Optional[str] = None, filters=None) -> int:
        schema, filters = self.coerce_schema_and_filters(schema, filters)
        where_clause, params = self._build_filters_clause(filters)
        qualified_table = self._qualify_table_sql(table_name, schema)
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}{where_clause}", params)
            count_result = cursor.fetchone()
            return int(count_result[0]) if count_result else 0
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            cursor.close()
            conn.close()

    def get_min_max_date(self, table_name: str, column_name: str, schema: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        qualified_table = self._qualify_table_sql(table_name, schema)
        try:
            sql = (
                f"SELECT MIN({self._quote_identifier(column_name)}), MAX({self._quote_identifier(column_name)}) "
                f"FROM {qualified_table} "
                f"WHERE {self._quote_identifier(column_name)} IS NOT NULL;"
            )
            cursor.execute(sql)
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cursor.close()
            conn.close()

    def extract_table_schema(self, table_name, schema: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        try:
            schema_sql = """
                SELECT
                    c.ORDINAL_POSITION AS position,
                    c.COLUMN_NAME AS name,
                    c.DATA_TYPE AS data_type,
                    c.CHARACTER_MAXIMUM_LENGTH AS max_length,
                    CASE WHEN c.IS_NULLABLE = 'YES' THEN 'YES' ELSE 'NO' END AS is_nullable,
                    c.COLUMN_DEFAULT AS default_value,
                    CASE WHEN k.CONSTRAINT_NAME = 'PRIMARY' THEN 'YES' ELSE 'NO' END AS is_primary_key,
                    CASE WHEN fk.CONSTRAINT_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_foreign_key,
                    CASE WHEN s.INDEX_NAME IS NOT NULL AND s.INDEX_NAME != 'PRIMARY' THEN 'YES' ELSE 'NO' END AS is_index
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage k
                    ON k.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND k.TABLE_NAME = c.TABLE_NAME
                    AND k.COLUMN_NAME = c.COLUMN_NAME
                    AND k.CONSTRAINT_NAME = 'PRIMARY'
                LEFT JOIN information_schema.key_column_usage fk
                    ON fk.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND fk.TABLE_NAME = c.TABLE_NAME
                    AND fk.COLUMN_NAME = c.COLUMN_NAME
                    AND fk.REFERENCED_TABLE_NAME IS NOT NULL
                LEFT JOIN information_schema.statistics s
                    ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
                    AND s.TABLE_NAME = c.TABLE_NAME
                    AND s.COLUMN_NAME = c.COLUMN_NAME
                    AND s.INDEX_NAME != 'PRIMARY'
                WHERE c.TABLE_SCHEMA = %s
                  AND c.TABLE_NAME = %s
                GROUP BY c.ORDINAL_POSITION, c.COLUMN_NAME, c.DATA_TYPE,
                         c.CHARACTER_MAXIMUM_LENGTH, c.IS_NULLABLE, c.COLUMN_DEFAULT,
                         k.CONSTRAINT_NAME, fk.CONSTRAINT_NAME, s.INDEX_NAME
                ORDER BY c.ORDINAL_POSITION;
            """

            cursor.execute(schema_sql, (target_schema, pure_table))
            rows = cursor.fetchall()

            seen = set()
            result = []
            for row in rows:
                col_name = row[1]
                if col_name in seen:
                    continue
                seen.add(col_name)

                result.append({
                    "position": row[0],
                    "name": col_name,
                    "type": cast_mysql_to_postgresql_type(row[2]),
                    "length": row[3],
                    "nullable": row[4],
                    "default": row[5],
                    "primary_key": row[6],
                    "foreign_key": row[7],
                    "is_index": row[8],
                })

            return result

        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()

    def fetch_deltas(self, cursor, primary_keys, log_table: str, since_ts: datetime, batch_size: int = 10_000, schema: Optional[str] = None):
        primary_keys = self.normalize_primary_keys(primary_keys)
        qualified_log_table = self._qualify_table_sql(log_table, schema)
        if not primary_keys:
            logger.error("fetch_deltas requires at least one primary key column.")
            return

        pk_select = ", ".join(self._quote_identifier(pk) for pk in primary_keys)
        pk_join = " AND ".join(
            f"t.{self._quote_identifier(pk)} = latest.{self._quote_identifier(pk)}"
            for pk in primary_keys
        )
        pk_order = ", ".join(f"t.{self._quote_identifier(pk)}" for pk in primary_keys)
        sql = f"""
            SELECT t.*
            FROM {qualified_log_table} t
            INNER JOIN (
                SELECT {pk_select}, MAX(`Date_operation`) AS max_op
                FROM {qualified_log_table}
                WHERE `Date_operation` > %s
                GROUP BY {pk_select}
            ) latest
            ON {pk_join}
            AND t.`Date_operation` = latest.max_op
            ORDER BY {pk_order}
            LIMIT %s OFFSET %s;
        """
        offset = 0
        while True:
            cursor.execute(sql, (since_ts, batch_size, offset))
            rows = cursor.fetchall()
            if not rows:
                break

            col_names = [desc[0] for desc in cursor.description]
            for row in rows:
                yield dict(zip(col_names, row))

            offset += batch_size

    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> bool:
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            qualified_table = self._qualify_table_sql(table_name, schema)
            cursor.execute(f"TRUNCATE TABLE {qualified_table}")
            conn.commit()
            logger.info(f"Successfully truncated table: {qualified_table}")
            return True
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_table_indexes(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return index definitions for a MySQL table.

        Output example:
        [
            {"name": "PRIMARY", "unique": True, "primary": True, "columns": ["id"]},
            {"name": "idx_name", "unique": False, "primary": False, "columns": ["name"]}
        ]
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        target_schema, pure_table = self.resolve_schema_and_table(table_name, schema)
        try:
            cursor.execute(
                """
                SELECT
                    INDEX_NAME,
                    NON_UNIQUE,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns_csv
                FROM information_schema.statistics
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                GROUP BY INDEX_NAME, NON_UNIQUE
                ORDER BY INDEX_NAME;
                """,
                (target_schema, pure_table),
            )
            rows = cursor.fetchall()

            results: List[Dict[str, Any]] = []
            for idx_name, non_unique, cols_csv in rows:
                is_primary = idx_name == "PRIMARY"
                is_unique = non_unique == 0
                cols = [c.strip() for c in (cols_csv or "").split(",") if c.strip()]
                if not cols:
                    continue
                results.append({
                    "name": idx_name,
                    "unique": is_unique,
                    "primary": is_primary,
                    "columns": cols,
                })
            return results

        except Exception as e:
            logger.error(f"Error getting indexes for MySQL table {self.database}.{table_name}: {e}")
            return []
        finally:
            cursor.close()
            conn.close()
