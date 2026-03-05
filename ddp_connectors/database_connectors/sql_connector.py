#!/usr/bin/python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from datetime import datetime
from typing import Iterator, Dict, Any

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
    
    @abstractmethod
    def get_connection_tables(self):
        """
        Returns a list of all table names in the given database.
        """

    @abstractmethod
    def get_connection_columns(self, table_name):
        """Returns a list of dictionaries with column names and types for the given table."""


    def get_database_schema(self):
        pass

    @abstractmethod
    def extract_data_batch(self, table_name: str, offset: int = 0, limit: int = 100):
        """
           Extracts a batch of rows from a table using SKIP/FIRST.
           Defaults to the first 100 rows if offset/limit are not provided.
        """

    @abstractmethod
    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        """
          Fetch up to `limit` rows from `table`, skipping the first `offset` rows.

        Args:
            table_name (str):       Name of the Informix table.
            offset (int):      Number of rows to skip.
            limit (int):       Maximum rows to return.
            cursor (Cursor):       An active database cursor. Must remain open; do not close it inside this method.

        Returns:
            list of tuple:     The fetched rows, empty if none remain.
        """

    @abstractmethod
    def extract_table_schema(self, table_name: str):
        """
           Gather column-level details from database including:
           - column name, type, nullability, default
           - primary key, foreign key, and index flags
        """

    @abstractmethod
    def fetch_deltas( self, cursor, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000) -> Iterator[Dict[str, Any]]:
        """
        Pull delta rows from <base_table>_log newer than since_ts.
        Uses LIMIT/OFFSET for pagination.
        """
        
    @abstractmethod
    def truncate_table(self, table_name: str) -> bool:
        """
        Remove all data from the specified table while keeping its structure
        """