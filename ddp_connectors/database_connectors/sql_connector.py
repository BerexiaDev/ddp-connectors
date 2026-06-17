#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from datetime import datetime
from typing import Iterator, Dict, Any, List, Optional, Tuple

from pyodbc import Cursor
from loguru import logger

class SqlConnector():

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.driver = None

    
    @abstractmethod
    def get_connection(self):
        """Returns a connection object from the driver."""


    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        conn = None
        cursor = None

        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # ensure the query actually ran
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        logger.info("Database connection is active.")
        return True

    def get_default_schema(self) -> Optional[str]:
        """Return the connector-level default schema when one exists."""
        return getattr(self, "schema", None)

    def _normalize_identifier(self, identifier: Optional[str]) -> Optional[str]:
        if identifier is None:
            return None

        normalized = str(identifier).strip()
        if not normalized:
            return None

        if normalized.startswith("[") and normalized.endswith("]"):
            normalized = normalized[1:-1]
        elif normalized[0] in {'"', "'", "`"} and normalized[-1] == normalized[0]:
            normalized = normalized[1:-1]

        return normalized.strip() or None

    def resolve_schema_and_table(self, table_name: str, schema: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Resolve explicit schema and already-qualified table names into a normalized pair.
        """
        normalized_schema = self._normalize_identifier(schema) or self.get_default_schema()
        normalized_table = (table_name or "").strip()

        if not normalized_table:
            return normalized_schema, normalized_table

        candidate = (
            normalized_table
            .replace("[", "")
            .replace("]", "")
            .replace('"', "")
            .replace("`", "")
        )
        parts = [part.strip() for part in candidate.split(".") if part.strip()]
        if len(parts) >= 2:
            return self._normalize_identifier(parts[-2]), self._normalize_identifier(parts[-1]) or ""

        return normalized_schema, self._normalize_identifier(normalized_table) or normalized_table

    def qualify_table_name(self, table_name: str, schema: Optional[str] = None) -> str:
        """
        Return a normalized schema-qualified name without double qualification.
        """
        resolved_schema, resolved_table = self.resolve_schema_and_table(table_name, schema)
        if resolved_schema:
            return f"{resolved_schema}.{resolved_table}"
        return resolved_table

    def normalize_primary_keys(self, primary_keys) -> List[str]:
        if primary_keys is None:
            return []
        if isinstance(primary_keys, str):
            return [primary_keys]
        return [str(pk).strip() for pk in primary_keys if str(pk).strip()]

    def coerce_schema_and_filters(self, schema=None, filters=None):
        """
        Keep backward compatibility with older calls where the second positional
        argument was `filters` instead of `schema`.
        """
        if filters is None and schema is not None and not isinstance(schema, str):
            return None, schema
        return schema, filters

    @abstractmethod
    def get_connection_schemas(self) -> List[str]:
        """
        Returns the selectable business schemas for the connector.
        """
    
    @abstractmethod
    def get_connection_tables(self, schema: Optional[str] = None):
        """
        Returns a list of all table names in the given database.
        """

    @abstractmethod
    def get_connection_columns(self, table_name, schema: Optional[str] = None):
        """Returns a list of dictionaries with column names and types for the given table."""


    def get_database_schema(self):
        pass

    @abstractmethod
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100, filters=None, schema: Optional[str] = None):
        """
           Extracts a batch of rows from a table using SKIP/FIRST.
           Defaults to the first 100 rows if offset/limit are not provided.
        """

    @abstractmethod
    def fetch_batch(self, cursor: Cursor, table_name, offset: int, batch_size: int = 100, schema: Optional[str] = None, **kwargs):
        """
          Fetch up to `batch_size` rows from `table`, skipping the first `offset` rows.

        Args:
            table_name (str):       Name of the Informix table.
            offset (int):      Number of rows to skip.
            batch_size (int):  Maximum rows to return.
            cursor (Cursor):       An active database cursor. Must remain open; do not close it inside this method.

        Returns:
            list of tuple:     The fetched rows, empty if none remain.
        """

    @abstractmethod
    def stream_batch(self, cursor: Cursor, table_name: str, batch_size: int = 10_000, schema: Optional[str] = None):
        """
        Yield rows in batches without loading the full table into memory.
        """

    @abstractmethod
    def extract_table_schema(self, table_name: str, schema: Optional[str] = None):
        """
           Gather column-level details from database including:
           - column name, type, nullability, default
           - primary key, foreign key, and index flags
        """

    @abstractmethod
    def count_table_rows(self, table_name: str, schema: Optional[str] = None, filters=None) -> int:
        """
        Count rows for the selected schema/table.
        """

    @abstractmethod
    def get_min_max_date(self, table_name: str, column_name: str, schema: Optional[str] = None):
        """
        Return the minimum and maximum values for a date/datetime column.
        """

    @abstractmethod
    def fetch_deltas(self, cursor, primary_keys, log_table: str, since_ts: datetime, batch_size: int = 10_000, schema: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Pull delta rows from <base_table>_log newer than since_ts.
        Uses LIMIT/OFFSET for pagination.
        """

    @abstractmethod
    def get_table_indexes(self, table_name: str, schema: Optional[str] = None):
        """
        Return source indexes for the selected schema/table.
        """
        
    @abstractmethod
    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Remove all data from the specified table while keeping its structure
        """
