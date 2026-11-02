"""Opt-in real ODBC tests. Settings are JSON in DDP_TEST_INFORMIX[_UTF8]_SETTINGS.

Only connection-local temporary tables are written. No existing fixtures change.
"""
from datetime import datetime
import json
import os
import unittest
import uuid

from ddp_connectors.connectors_factory import ConnectorFactory


class InformixLiveMixin:
    settings_variable = "DDP_TEST_INFORMIX_SETTINGS"
    unicode_values = ["entity", "caf\u00e9", "O'Brien"]

    def setUp(self):
        settings = json.loads(os.environ[self.settings_variable])
        self.connector = ConnectorFactory().create_connector("informix", settings)
        if os.environ.get("DDP_TEST_INFORMIX_DRIVER"):
            self.connector.driver_path = os.environ["DDP_TEST_INFORMIX_DRIVER"]
        self.connection = self.connector.get_connection()
        self.addCleanup(self.connection.close)
        self.cursor = self.connection.cursor()
        self.addCleanup(self.cursor.close)

    def test_database_codeset_matches_configured_fixture(self):
        self.cursor.execute("SELECT dbs_collate FROM sysmaster:sysdbslocale WHERE dbs_dbsname = ?", [self.connector.database])
        codeset = self.cursor.fetchone()[0].strip().rsplit(".", 1)[1]
        self.assertEqual(codeset, "57372" if self.settings_variable.endswith("UTF8_SETTINGS") else "819")

    def test_bound_filter_and_text_keys_survive_storage_and_retrieval(self):
        self.assertEqual([ord(c) for c in "caf\u00e9"], [99, 97, 102, 233])
        table = "ddpc_" + uuid.uuid4().hex[:16]
        self.cursor.execute(f"CREATE TEMP TABLE {table} (text_key VARCHAR(80), kind VARCHAR(80), wide NVARCHAR(80)) WITH NO LOG")
        for value in self.unicode_values:
            with self.subTest(value=value):
                self.cursor.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", [value, value, value])
                self.cursor.execute(f"SELECT text_key, kind, wide FROM {table} WHERE kind = ? AND text_key = ?", [value, value])
                self.assertEqual([tuple(row) for row in self.cursor.fetchall()], [(value, value, value)])
                self.cursor.execute("SELECT CAST(? AS VARCHAR(80)) FROM systables WHERE tabid = 1", [value])
                self.assertEqual(self.cursor.fetchone()[0], value)

    def test_numeric_and_timestamp_parameters(self):
        stamp = datetime(2026, 11, 2, 12, 34, 56, 123450)
        self.cursor.execute("SELECT tabid FROM systables WHERE tabid = ?", [1])
        self.assertEqual(self.cursor.fetchone()[0], 1)
        self.cursor.execute("SELECT CAST(? AS DATETIME YEAR TO FRACTION(5)) FROM systables WHERE tabid = 1", [stamp])
        self.assertEqual(self.cursor.fetchone()[0], stamp)


@unittest.skipUnless(os.environ.get("DDP_TEST_INFORMIX_SETTINGS"), "No Latin-1 Informix configured")
class InformixLatin1LiveTests(InformixLiveMixin, unittest.TestCase):
    def test_unrepresentable_text_raises_without_replacement(self):
        with self.assertRaises(UnicodeEncodeError):
            self.cursor.execute("SELECT CAST(? AS VARCHAR(80)) FROM systables WHERE tabid = 1", ["\u20ac"])
        self.cursor.execute("SELECT CAST(? AS VARCHAR(80)) FROM systables WHERE tabid = 1", ["caf\u00e9"])
        self.assertEqual(self.cursor.fetchone()[0], "caf\u00e9")


@unittest.skipUnless(os.environ.get("DDP_TEST_INFORMIX_UTF8_SETTINGS"), "No UTF-8 Informix configured")
class InformixUtf8LiveTests(InformixLiveMixin, unittest.TestCase):
    settings_variable = "DDP_TEST_INFORMIX_UTF8_SETTINGS"
    unicode_values = ["entity", "caf\u00e9", "O'Brien", "\u20ac", "\u4e2d\u6587"]

    def test_invalid_unicode_raises_without_replacement(self):
        with self.assertRaises(UnicodeEncodeError):
            self.cursor.execute("SELECT CAST(? AS VARCHAR(80)) FROM systables WHERE tabid = 1", ["\ud800"])
