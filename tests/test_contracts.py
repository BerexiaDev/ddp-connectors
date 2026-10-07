"""Consumer-facing contracts, independent of live database services."""
import unittest
from unittest.mock import Mock

from ddp_connectors.connectors_factory import ConnectorFactory
from ddp_connectors.database_connectors.postgres_connector import PostgresConnector
from ddp_connectors.database_connectors.sql_connector_utils import safe_convert_to_string


def column(name="id", type="INTEGER", **overrides):
    return {"name": name, "type": type, "nullable": "NO", "default": "7",
            "primary_key": "YES", "is_index": "NO", **overrides}


class ConnectorContracts(unittest.TestCase):
    def setUp(self):
        self.pg = PostgresConnector("host", "user", "password", 5432, "database", "public")

    def test_factory_keeps_existing_settings_and_types(self):
        settings = dict(host="host", user="user", password="password", port=1234,
                        database="database", protocol="onsoctcp", locale="en_US.utf8")
        expected = {"postgres": "PostgresConnector", "sqlserver": "SqlServerConnector",
                    "informix": "InformixConnector", "oracle": "OracleConnector",
                    "mongo": "MongoConnector", "mysql": "MySQLConnector", "db2i": "Db2iConnector"}
        for kind, name in expected.items():
            with self.subTest(kind=kind):
                connector = ConnectorFactory().create_connector(kind, settings)
                self.assertEqual(type(connector).__name__, name)
                self.assertEqual(getattr(connector, "database_name", None) if kind == "mongo"
                                 else connector.database, "database")
        self.assertEqual(ConnectorFactory().create_connector("postgres", settings).schema, "public")
        self.assertEqual(ConnectorFactory().create_connector("postgres", {**settings, "schema": "chosen"}).schema, "chosen")

    def test_ordinary_ddl_keeps_pk_types_and_no_defaults(self):
        columns = [column(), {**column("label", "VARCHAR"), "length": 24,
                              "primary_key": "NO", "is_index": "YES"}]
        create, indexes = self.pg.build_create_table_statement("items", "public", columns)
        self.assertEqual(create, 'CREATE TABLE IF NOT EXISTS "public"."items" (\n  "id" INTEGER NOT NULL,\n  "label" VARCHAR(24) NOT NULL,\n  PRIMARY KEY ("id")\n);')
        self.assertEqual(indexes, 'CREATE INDEX IF NOT EXISTS "idx_public_items_label" ON "public"."items" ("label");')
        self.assertEqual(self.pg.build_create_table_statement("empty"),
                         ('CREATE TABLE IF NOT EXISTS "public"."empty" (\n  \n);', None))

    def test_schema_and_legacy_filter_arguments(self):
        filters = [{"column": "id", "value": 1}]
        self.assertEqual(self.pg.coerce_schema_and_filters(filters), (None, filters))
        self.assertEqual(self.pg.coerce_schema_and_filters("selected", filters), ("selected", filters))
        self.assertEqual(self.pg.resolve_schema_and_table('"chosen"."items"'), ("chosen", "items"))
        self.assertEqual(self.pg.normalize_primary_keys("id"), ["id"])
        self.assertEqual(self.pg.normalize_primary_keys(["id", "other"]), ["id", "other"])

    def test_byte_conversion_retains_replacement_behavior(self):
        self.assertEqual(safe_convert_to_string(b"a\xff"), "a\ufffd")
        self.assertIsNone(safe_convert_to_string(None))

    def test_fetch_batch_keeps_limit_alias_and_borrowed_cursor(self):
        cursor = Mock()
        cursor.fetchall.return_value = [(1, "value")]
        self.assertEqual(self.pg.fetch_batch(cursor, "items", 4, 50, "selected", limit=2),
                         [(1, "value")])
        cursor.execute.assert_called_once_with('SELECT * FROM "selected"."items" OFFSET 4 LIMIT 2')
        cursor.close.assert_not_called()


class FileConnectorContracts(unittest.TestCase):
    def test_current_factory_rejects_unknown_types(self):
        with self.assertRaises(ValueError):
            ConnectorFactory().create_connector("unsupported", {})

    def test_sftp_factory_is_lazy_and_read_only(self):
        connector = ConnectorFactory().create_connector("sftp", {
            "host": "files.example.test", "user": "reader", "password": "test-password",
            "remote_root": "/exports/", "known_hosts": "known_hosts"})
        self.assertEqual(type(connector).__name__, "SftpConnector")
        self.assertEqual(connector.remote_root, "/exports")
        self.assertEqual(connector.port, 22)
        self.assertIsNone(connector._sftp)
        self.assertFalse(hasattr(connector, "insert_data"))
        connector.close()
        connector.close()


if __name__ == "__main__":
    unittest.main()
