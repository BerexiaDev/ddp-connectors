"""PR #16 ping/error behavior must coexist with the shared retry contracts."""
import unittest
from unittest.mock import Mock, patch

import pyodbc

from ddp_connectors.database_connectors.informix_connector import InformixConnector
from ddp_connectors.database_connectors.oracle_connector import OracleConnector
from ddp_connectors.database_connectors.sql_connector import SqlConnector


class PingContracts(unittest.TestCase):
    def test_generic_ping_keeps_select_one_and_closes_owned_handles(self):
        connector = SqlConnector("host", "user", "password", 1, "db")
        connector.last_error = "previous failure"
        connection = Mock()
        with patch.object(connector, "_open_connection", return_value=connection):
            self.assertTrue(connector.ping())
        connection.cursor.return_value.execute.assert_called_once_with("SELECT 1")
        connection.cursor.return_value.close.assert_called_once()
        connection.close.assert_called_once()
        self.assertIsNone(connector.last_error)

    def test_informix_ping_uses_from_clause(self):
        connector = InformixConnector("host", "user", "password", 9088, "db", "onsoctcp", "en_US.819")
        connection = Mock()
        def execute(query):
            if query != "SELECT 1 FROM systables WHERE tabid = 1":
                raise pyodbc.ProgrammingError("42000", "Informix syntax error (-201)")
        connection.cursor.return_value.execute.side_effect = execute
        with patch.object(connector, "_open_connection", return_value=connection):
            self.assertTrue(connector.ping())
        connection.close.assert_called_once()
        self.assertIsNone(connector.last_error)

    def test_informix_driver_error_is_readable_and_clears_after_success(self):
        connector = InformixConnector("host", "user", "password", 9088, "db", "onsoctcp", "en_US.819")
        error = pyodbc.Error("7000", "[7000] [DataDirect][ODBC 20101 driver]6015 (0) (SQLDriverConnect)")
        with patch.object(connector, "_open_connection", side_effect=error) as connect:
            self.assertFalse(connector.ping())
        connect.assert_called_once()
        self.assertIn("Could not connect to the Informix server", connector.last_error)
        self.assertIn("6015", connector.last_error)
        with patch.object(connector, "_open_connection", return_value=Mock()):
            self.assertTrue(connector.ping())
        self.assertIsNone(connector.last_error)

    def test_informix_native_and_unknown_error_descriptions(self):
        connector = InformixConnector("host", "user", "password", 9088, "db", "onsoctcp", "en_US.819")
        for code, meaning in [("-201", "SQL syntax error"), ("-206", "table not found"),
                              ("-329", "database not found"), ("-387", "no connect permission"),
                              ("-951", "incorrect password")]:
            with self.subTest(code=code):
                description = connector.describe_error(pyodbc.Error("5100", f"[DataDirect]10801 ({code}) (SQLExecDirectW)"))
                self.assertIn(code, description)
                self.assertIn(meaning, description)
        self.assertEqual(connector.describe_error(RuntimeError("unknown failure")), "unknown failure")
        self.assertEqual(connector.describe_error(Exception()), "")

    def test_retry_success_clears_error_and_closes_both_attempts(self):
        connector = SqlConnector("host", "user", "password", 1, "db")
        connector.retry_backoff_seconds = 0
        connector.last_error = "old failure"
        first, second = Mock(), Mock()
        first.cursor.return_value.execute.side_effect = pyodbc.OperationalError("08S01", "connection reset")
        with patch.object(connector, "_open_connection", side_effect=[first, second]):
            self.assertTrue(connector.ping())
        self.assertIsNone(connector.last_error)
        for connection in (first, second):
            connection.close.assert_called_once()
            connection.cursor.return_value.close.assert_called_once()

    def test_exhausted_retry_records_final_error_and_closes_handles(self):
        connector = SqlConnector("host", "user", "password", 1, "db")
        connector.max_connection_retries = 1
        connector.retry_backoff_seconds = 0
        connections = [Mock(), Mock()]
        for index, connection in enumerate(connections):
            connection.cursor.return_value.execute.side_effect = pyodbc.OperationalError("08S01", f"connection reset {index}")
        with patch.object(connector, "_open_connection", side_effect=connections):
            self.assertFalse(connector.ping())
        self.assertEqual(connector.last_error, "connection reset 1")
        for connection in connections:
            connection.close.assert_called_once()
            connection.cursor.return_value.close.assert_called_once()

    def test_oracle_missing_schema_and_failure_set_error_and_success_clears_it(self):
        connector = OracleConnector("host", "user", "password", 1521, "db", "reporting")
        connection = Mock()
        connection.cursor.return_value.fetchone.side_effect = [(0,), (1,)]
        with patch.object(connector, "_open_connection", return_value=connection):
            self.assertFalse(connector.ping())
            self.assertIn("REPORTING", connector.last_error)
            self.assertTrue(connector.ping())
            self.assertIsNone(connector.last_error)
        with patch.object(connector, "_open_connection", side_effect=RuntimeError("connection rejected")):
            self.assertFalse(connector.ping())
        self.assertEqual(connector.last_error, "connection rejected")
