"""Configuration, failure cleanup and retry contracts; real text tests are live."""
import unittest
from unittest.mock import Mock, patch

import pyodbc

from ddp_connectors.database_connectors.informix_connector import InformixConnector


def connector(locale="en_US.819"):
    return InformixConnector("host", "user", "password", 9088, "db", "onsoctcp", locale)


class InformixEncodingTests(unittest.TestCase):
    def test_supported_locale_configures_narrow_text_consistently(self):
        for locale, codec in [("en_US.819", "latin1"), ("fr_FR.8859-1", "latin1"),
                              ("en_US.utf8", "utf-8"), ("en_us.UTF8", "utf-8"),
                              ("en_US.57372", "utf-8")]:
            with self.subTest(locale=locale), patch("pyodbc.connect") as connect:
                conn = connector(locale).get_connection()
                conn.setencoding.assert_called_once_with(encoding=codec, ctype=pyodbc.SQL_CHAR)
                for sqltype in (pyodbc.SQL_CHAR, pyodbc.SQL_WCHAR):
                    conn.setdecoding.assert_any_call(sqltype, encoding=codec, ctype=pyodbc.SQL_CHAR)
                self.assertIn(f"CLIENT_LOCALE={locale};DB_LOCALE={locale};", connect.call_args.args[0])
                conn.close.assert_not_called()

    def test_ambiguous_or_unsupported_locale_fails_before_connecting(self):
        for locale in [None, "", "en_US", "en_US.1252", "en_US.unknown", "en_US.819@x",
                       "en_US.819;DB_LOCALE=en_US.utf8", 819]:
            with self.subTest(locale=locale), patch("pyodbc.connect") as connect:
                with self.assertRaisesRegex(ValueError, "Informix locale"):
                    connector(locale).get_connection()
                connect.assert_not_called()

    def test_each_setup_failure_closes_connection_without_retrying(self):
        for step in ["encoding", "char", "wchar"]:
            for failure in [ValueError("bad codec"), KeyboardInterrupt()]:
                with self.subTest(step=step, failure=type(failure).__name__):
                    conn = Mock()
                    if step == "encoding":
                        conn.setencoding.side_effect = failure
                    else:
                        conn.setdecoding.side_effect = [failure] if step == "char" else [None, failure]
                    with patch("pyodbc.connect", return_value=conn) as connect:
                        with self.assertRaises(type(failure)):
                            connector().get_connection()
                    conn.close.assert_called_once()
                    self.assertEqual(connect.call_count, 1)

    def test_transient_setup_failure_closes_then_retries(self):
        first, second = Mock(), Mock()
        first.setencoding.side_effect = pyodbc.OperationalError("08S01", "connection reset")
        instance = connector()
        instance.retry_backoff_seconds = 0
        with patch("pyodbc.connect", side_effect=[first, second]):
            self.assertIs(instance.get_connection(), second)
        first.close.assert_called_once()
        second.close.assert_not_called()

    def test_borrowed_fetch_cursor_stays_open(self):
        cursor = Mock()
        cursor.fetchall.return_value = [("caf\u00e9",)]
        instance = connector()
        with patch.object(instance, "_open_connection") as connect:
            self.assertEqual(instance.fetch_batch(cursor, "items", 0, 1), [("caf\u00e9",)])
        connect.assert_not_called()
        cursor.close.assert_not_called()
