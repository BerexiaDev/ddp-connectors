#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import re, psycopg2
from datetime import datetime

from loguru import logger
from typing import Dict, Any, List, Tuple
from pyodbc import Cursor

from .sql_connector import SqlConnector
from cmr_connectors_lib.database_connectors.utils.postgres_connector_utils import _build_select_clause, _build_joins_clause, _build_where_clause, _build_group_by, \
    _build_having_clause
from cmr_connectors_lib.database_connectors.sql_connector_utils import cast_postgres_to_typescript
from .sql_connector_utils import safe_convert_to_string


class PostgresConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, schema):
        super().__init__(host, user, password, port, database)
        self.driver = "postgresql+psycopg2"
        self.schema = schema
    
    def get_connection(self):
        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "port": self.port,
            "dbname": self.database,
            "connect_timeout": 10,
            "options" : f"-c search_path={self.schema}"
        }

        return psycopg2.connect(**conn_params)

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
                "MATCHES": ("~", lambda v: v),
                "NOT_MATCHES": ("!~", lambda v: v),
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
                    clauses.append(f"\"{col_name}\" {sql_op} %s AND %s")
                    params.extend([value, value_to])
                elif op_key in ("IN", "NOT_IN"):
                    values = value
                    if isinstance(values, str):
                        values = [v.strip() for v in values.split(",") if v.strip()]
                    if not isinstance(values, list) or not values:
                        logger.warning(f"Skipping IN filter for '{col_name}' due to empty values.")
                        continue
                    placeholders = ", ".join(["%s"] * len(values))
                    clauses.append(f"\"{col_name}\" {sql_op} ({placeholders})")
                    params.extend(values)
                elif op_key in ("IS_NULL", "IS_NOT_NULL"):
                    clauses.append(f"\"{col_name}\" {sql_op}")
                elif isinstance(sql_op, tuple):
                    sql_operator, pattern_builder = sql_op
                    if value is None:
                        logger.warning(f"Skipping filter for '{col_name}' because value is missing.")
                        continue
                    clauses.append(f"\"{col_name}\" {sql_operator} %s")
                    params.append(pattern_builder(value))
                else:
                    if value is None:
                        logger.warning(f"Skipping filter for '{col_name}' because value is missing.")
                        continue
                    clauses.append(f"\"{col_name}\" {sql_op} %s")
                    params.append(value)

        where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_clause, params


    def extract_data_batch( self, table_name: str, offset: int = 0, limit: int = 100, filters=None) -> List[Dict[str, Any]]:
        where_clause, params = self._build_filters_clause(filters)
        query = (
            f"SELECT * FROM {table_name}"
            f"{where_clause} "
            f"OFFSET {offset} LIMIT {limit};"
        )
        logger.info(f"Fetching batch: table={table_name}, offset={offset}, limit={limit}, filters_applied={bool(where_clause.strip())}")
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

    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        try:
            query = f'SELECT * FROM {table_name} OFFSET {offset} LIMIT {limit}'
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []

    def stream_batch(self, table_name: str, batch_size: int = 10_000,):
        """
        Full-sync streaming for Postgres using a server-side cursor.
        Avoids OFFSET and prevents loading the full result set into memory.
        """
        conn = None
        cursor = None
        cursor_name = f"stream_{table_name.replace('.', '_')}"

        try:
            conn = self.get_connection()
            # IMPORTANT: server-side cursors require an open transaction
            cursor = conn.cursor(name=cursor_name)
            cursor.itersize = batch_size

            logger.info(f"Start streaming Postgres table {table_name} with batch_size={batch_size}")
            cursor.execute(f"SELECT * FROM {table_name}")

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

            logger.info(f"Finished streaming Postgres table {table_name}")

        except Exception as exc:
            logger.error(f"Error streaming batch from Postgres table {table_name}: {exc}")
            return

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_connection_tables(self):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type   = 'BASE TABLE';
                """,
                (self.schema,),
            )
            return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def get_connection_columns(self, table_name: str):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                  column_name,
                  data_type,
                  udt_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name   = %s
                ORDER BY ordinal_position;
                """,
                (self.schema, table_name),
            )
            rows = cur.fetchall()

            columns: list[dict[str, str]] = []
            for column_name, data_type, udt_name in rows:
                ts_type = cast_postgres_to_typescript(data_type)
                columns.append({"name": column_name, "type": ts_type, "alias": column_name})
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
        finally:
            cur.close()
            conn.close()


    def count_table_rows(self, table_name: str, filters=None) -> int:
        where_clause, params = self._build_filters_clause(filters)
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}{where_clause}", params)
            count_result = cursor.fetchone()
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
        Returns (min_value, max_value) for a DATE/TIMESTAMP column in a Postgres table.
        Assumes table_name / column_name are valid identifiers (same assumption as other methods).
        """

        conn = self.get_connection()
        cur = conn.cursor()
        try:
            sql = (
                f'SELECT MIN("{column_name}"), MAX("{column_name}") '
                f'FROM {table_name} '
                f'WHERE "{column_name}" IS NOT NULL;'
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
        try:
            schema_sql = """
                      SELECT
                        a.attnum AS position,
                        a.attname AS name,
                        format_type(a.atttypid, a.atttypmod) AS data_type,
                        a.attlen AS max_length,
                        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                        COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '') AS default_value,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_index i
                                WHERE i.indrelid = c.oid
                                  AND i.indisprimary
                                  AND a.attnum = ANY(i.indkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_primary_key,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_constraint fk
                                WHERE fk.contype = 'f'
                                  AND fk.conrelid = c.oid
                                  AND a.attnum = ANY(fk.conkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_foreign_key,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM pg_index i2
                                WHERE i2.indrelid = c.oid
                                  AND NOT i2.indisprimary
                                  AND a.attnum = ANY(i2.indkey)
                            )
                            THEN 'YES'
                            ELSE 'NO'
                        END AS is_index
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_type t ON a.atttypid = t.oid
                    LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %s
                      AND c.relname = %s
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum;
                """

            cursor.execute(schema_sql, (self.schema, table_name))
            rows = cursor.fetchall()

            result = [
                {
                    "position": row[0],
                    "name": row[1],
                    "type": row[2].upper(),
                    "length": row[3],
                    "nullable": row[4],
                    "default": row[5],
                    "primary_key": row[6],
                    "foreign_key": row[7],
                    "is_index": row[8],
                }
                for row in rows
            ]

            return result

        except Exception as exc:
            logger.error(f"Error extracting schema for {table_name}: {exc}")
            return []
        finally:
            cursor.close()
            conn.close()


    def create_schema_if_missing(self, schema_name: str):
        """Creates a schema in PostgreSQL if it doesn't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
            conn.commit()
            logger.info(f"Schema {schema_name} created or already exists.")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

            
    def create_table_if_missing(self, table_name:str, create_table_statement: str, index_table_statement:str = None):
        """Creates a table in PostgreSQL if it doesn't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(create_table_statement)
            if index_table_statement:
                cursor.execute(index_table_statement)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists.")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def build_create_partitioned_table_statement(
        self,
        table_name: str,
        schema_name: str,
        columns: list,
        partition_column: str,
        partition_key: str,                 # NEW
        partition_method: str = "RANGE",
    ):
        """
        Build CREATE TABLE for a partitioned parent table, with a composite PK:
        PRIMARY KEY (partition_key, partition_column)
        then append:
        PARTITION BY <method> ("<partition_column>")
        """
        method = (partition_method or "RANGE").upper()
        if method not in ("RANGE", "LIST", "HASH"):
            raise ValueError(f"Unsupported partition method: {method}")

        create_stmt, index_stmt = self.build_create_table_statement(table_name, schema_name, columns)

        # Inject composite PRIMARY KEY before closing ');'
        pk_sql = f'PRIMARY KEY ("{partition_key}", "{partition_column}")'
        create_stmt = re.sub(
            r"\n\);\s*$",
            f"\n,  {pk_sql}\n);",
            create_stmt,
        )

        # Transform the ending ");" into ") PARTITION BY ...;"
        create_stmt = re.sub(
            r"\n\);\s*$",
            f'\n) PARTITION BY {method} ("{partition_column}");',
            create_stmt,
        )

        return create_stmt, index_stmt


    def _partition_table_name(self, parent_table: str, partition_column: str, label: str) -> str:
        # make label safe for identifiers (e.g. "2025-01" -> "2025_01")
        safe_label = re.sub(r"[^A-Za-z0-9_]", "_", str(label))
        return f"{parent_table}__p_{partition_column}__{safe_label}".lower()

    def create_default_partition(self, schema_name: str, parent_table: str) -> None:

        default_table = f"{parent_table}__p_default".lower()
        sql_txt = (
            f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{default_table}" '
            f'PARTITION OF "{schema_name}"."{parent_table}" DEFAULT;'
        )

        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql_txt)
            conn.commit()
            logger.info(f"Ensured DEFAULT partition: {schema_name}.{default_table}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed creating DEFAULT partition for {schema_name}.{parent_table}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def create_range_partitions_year_month(
        self,
        schema_name: str,
        parent_table: str,
        partition_column: str,
        strategy: str,
        ranges: List[str],
    ) -> None:
        """
        Create RANGE partitions for strategy in {"year","month"}.
        ranges:
          - year:  ["2000","2001",...]
          - month: ["2025-01","2025-02",...]
        """

        strat = (strategy or "").strip().lower()
        if strat not in ("year", "month"):
            raise ValueError(f"Unsupported partition strategy: {strategy}")

        if not ranges:
            raise ValueError(f"Partition enabled but ranges list is empty for {schema_name}.{parent_table}")

        conn = self.get_connection()
        cur = conn.cursor()
        try:
            for label in ranges:
                if strat == "year":
                    # label: YYYY
                    start = datetime.strptime(str(label), "%Y").date()
                    end = datetime.strptime(str(int(label) + 1), "%Y").date()
                else:
                    # label: YYYY-MM
                    start = datetime.strptime(str(label), "%Y-%m").date()
                    y, m = start.year, start.month
                    if m == 12:
                        end = datetime(y + 1, 1, 1).date()
                    else:
                        end = datetime(y, m + 1, 1).date()

                part_table = self._partition_table_name(parent_table, partition_column, str(label))

                create_part_sql = (
                    f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{part_table}" '
                    f'PARTITION OF "{schema_name}"."{parent_table}" '
                    f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');"
                )
                cur.execute(create_part_sql)

            conn.commit()
            logger.info(
                f"Created/verified {len(ranges)} partitions for {schema_name}.{parent_table} "
                f"on {partition_column} strategy={strat}"
            )
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed creating partitions for {schema_name}.{parent_table}: {e}")
            raise
        finally:
            cur.close()
            conn.close()


    
    def  build_create_table_statement(self, table_name: str, schema_name: str = 'public', columns = []):
        """
        Generates a PostgreSQL CREATE TABLE statement along with a CREATE INDEX statement
        (for indexed columns) using the provided column metadata.
        """
        column_defs = []
        primary_keys = []
        index_keys = []
        # matches func_name(…)  or   schema.func_name(…)
        fun_call = re.compile(r'^[A-Za-z_][\w\.]*\s*\(.*\)$')
        for col in columns:
            col_name = col["name"]
            col_type = col["type"].upper()
            length = col.get("length")
            nullable = col["nullable"].strip() == "YES"
            default = col["default"] or ""
            is_pk = col["primary_key"].strip() == "YES"
            if col['is_index'] == "YES":
                index_keys.append(col_name)

            # Handle types with length
            if col_type in ("VARCHAR", "CHAR") and length:
                col_type_str = f"{col_type}({length})"
            else:
                col_type_str = col_type

            # Build column definition
            col_def_parts = [f'"{col_name}"', col_type_str]

            if not nullable:
                col_def_parts.append("NOT NULL")

                # DEFAULT handling
            if default:
                d = default
                # is this a function call?  (unquoted identifier + '(')
                if not fun_call.match(d):
                    # it’s either a literal ('…'), numeric (1234), casted literal ('…'::text), etc.
                    col_def_parts.append(f"DEFAULT {d}")
                # else: skip it

            column_defs.append(" ".join(col_def_parts))

            # if is_pk:
            #     primary_keys.append(f'"{col_name}"')

        # Append primary key constraint
        if primary_keys:
            pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
            column_defs.append(pk_def)

        columns_sql = ",\n  ".join(column_defs)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (\n  {columns_sql}\n);'
        index_stmt = None
        if index_keys:
            index_statements = [
                (
                    f'CREATE INDEX IF NOT EXISTS "idx_{schema_name}_{table_name}_{col}" '
                    f'ON "{schema_name}"."{table_name}" ("{col}")'
                )
                for col in index_keys
            ]
            index_stmt = ";\n".join(index_statements) + ";"

        return create_stmt, index_stmt


    def get_view_columns(self, table_name: str, schema_name: str = 'populations'):
        """
           Returns a list of dicts with column names and mapped TypeScript types
           for the given Postgres view in the given schema.

           Args:
               table_name: The name of the table to get columns for
               schema: Schema name, defaults to "populations"
        """
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                    SELECT
                        a.attname AS column_name,
                        trim( split_part( pg_catalog.format_type(a.atttypid, a.atttypmod)
                                        , '(' , 1) ) AS data_type
                    FROM   pg_attribute   AS a
                    JOIN   pg_class       AS c ON c.oid          = a.attrelid
                    JOIN   pg_namespace   AS n ON n.oid          = c.relnamespace
                    WHERE  c.relkind   = 'm'
                      AND  n.nspname   = %s
                      AND  c.relname   = %s
                      AND  a.attnum    > 0
                      AND  NOT a.attisdropped
                    ORDER  BY a.attnum;
                  """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()

            columns: list[dict[str, str]] = []
            for column_name, data_type in rows:
                ts_type = cast_postgres_to_typescript(data_type)
                columns.append({"name": column_name, "type": ts_type})
            return columns
        except Exception as e:
            logger.error(f"Error getting view columns: {e}")
            return []

        finally:
            cur.close()
            conn.close()

    def fetch_deltas(self, cursor, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000):
        sql = f"""
            SELECT DISTINCT ON ({primary_key}) *
            FROM {log_table}
            WHERE Date_operation > %s
            ORDER BY {primary_key}, 
            Date_operation DESC
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


    def build_query(self, data: Dict[str, Any], invert_where: bool = False):
        """
        Build an Informix SQL query based on the provided JSON definition.
        """
        try:
            # Step 1: Validate input data
            base_table = data.get('baseTable')
            if not base_table:
                logger.error("Base table is required")
                return None

            # Step 4: Build the SELECT clause
            select_clause = _build_select_clause(data.get('selectedFields', []))

            # Step 5: Build the FROM
            from_clause = f"FROM {base_table}"
            joins_clause = _build_joins_clause(base_table, data.get('joins', []))

            # Step 6: Build the WHERE clause
            where_clause = _build_where_clause(data.get('whereConditions', []), invert_where)

            # Step 7: Build the GROUP BY clause
            group_by_clause = _build_group_by(data.get('groupByFields', []))
            having_clause = _build_having_clause(data.get('having', []))

            # Combine clauses into a list
            clauses = [
                select_clause,
                from_clause,
                joins_clause,
                where_clause,
                group_by_clause,
                having_clause
            ]

            # Join the clauses with a newline if the clause is not empty.
            query = "\n".join(clause for clause in clauses if clause.strip())
            return query

        except Exception as e:
            logger.error(f"Error building query: {str(e)}")
            return None
        
        
    def truncate_table(self, table_name: str, schema: str = None) -> bool:
        """
        Remove all data from the specified table while keeping its structure
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            use_schema = schema if schema else self.schema
            truncate_sql = f'TRUNCATE TABLE "{use_schema}"."{table_name}"'
            cursor.execute(truncate_sql)
            conn.commit()
            logger.info(f"Successfully truncated table: {use_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to truncate table {use_schema}.{table_name}: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def manage_table_indexes(
            self,
            table_name: str,
            columns: List[str],
            schema = None,
            create: bool = True
    ) -> None:
        """
        Create or drop a separate index on each column in `columns` for the given Postgres table.

        Index names follow: <schema>_<table_name>_idx_<column>.

        :param create: True to create indexes, False to drop them.
        """
        conn = None
        cursor = None
        use_schema = schema or self.schema
        qualified_table = f'"{use_schema}"."{table_name}"'

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            for col in columns:
                index_name = f"idx_{use_schema}_{table_name}_{col}"

                if create:
                    sql = (
                        f'CREATE INDEX IF NOT EXISTS "{index_name}" '
                        f'ON {qualified_table} ("{col}");'
                    )
                    action = "Created or verified"
                else:
                    sql = f'DROP INDEX IF EXISTS "{use_schema}"."{index_name}";'
                    action = "Dropped"

                cursor.execute(sql)
                logger.info(f"{action} index: {index_name}")

            conn.commit()

        except Exception as e:
            logger.error(f"Error managing indexes on {use_schema}.{table_name}: {e}")
            if conn:
                conn.rollback()

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def manage_table_primary_keys(
            self,
            table_name: str,
            columns: List[str],
            schema = None,
            create: bool = True
    ) -> Tuple[Dict[str, str], int]:
        """
        Create or drop a primary key constraint for the given Postgres table.

        :param columns: The columns that compose the primary key when create=True.
        :param create: True to recreate the primary key, False to drop it.
        :return: (response_dict, status_code)
        """
        use_schema = schema or self.schema
        qualified_table = f'"{use_schema}"."{table_name}"'
        constraint_name = f"pk_{use_schema}_{table_name}"

        if create and not columns:
            logger.error(
                f"Cannot create primary key on {use_schema}.{table_name}: no columns provided."
            )
            return {"status": "error", "message": "Failed to create data mart connector"}, 500

        conn = None
        cursor = None

        def _drop_existing_constraints() -> List[str]:
            cursor.execute(
                """
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = %s
                  AND table_name = %s
                  AND constraint_type = 'PRIMARY KEY';
                """,
                (use_schema, table_name),
            )
            constraints = [row[0] for row in cursor.fetchall()]
            for existing_constraint in constraints:
                drop_sql = (
                    f'ALTER TABLE {qualified_table} '
                    f'DROP CONSTRAINT IF EXISTS "{existing_constraint}";'
                )
                cursor.execute(drop_sql)
                logger.info(f"Dropped primary key constraint: {existing_constraint}")
            return constraints

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if create:
                _drop_existing_constraints()
                columns_list = ", ".join(f'"{col}"' for col in columns)
                create_sql = (
                    f'ALTER TABLE {qualified_table} '
                    f'ADD CONSTRAINT "{constraint_name}" PRIMARY KEY ({columns_list});'
                )
                cursor.execute(create_sql)
                logger.info(f"Created primary key constraint: {constraint_name}")
                message = f"Primary key created for columns: {', '.join(columns)}"
            else:
                dropped = _drop_existing_constraints()
                if dropped:
                    message = f"Dropped primary key constraint(s): {', '.join(dropped)}"
                else:
                    message = "No primary key found to drop"

            conn.commit()
            return {"status": "success", "message": message}, 200

        except Exception as e:
            logger.error(f"Error managing primary keys on {use_schema}.{table_name}: {e}")
            if conn:
                conn.rollback()
            return {"status": "error", "message": "Failed to create data mart connector"}, 500

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
