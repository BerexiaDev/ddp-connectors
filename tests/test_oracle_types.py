"""Oracle catalog type names must produce usable PostgreSQL declarations."""
import unittest
from unittest.mock import MagicMock, patch

from ddp_connectors.database_connectors.oracle_connector import OracleConnector
from ddp_connectors.database_connectors.postgres_connector import PostgresConnector
from ddp_connectors.database_connectors.sql_connector_utils import cast_oracle_to_postgresql_type


class OracleTypeTests(unittest.TestCase):
    def test_timestamp_precision_and_timezone_variants(self):
        for suffix, expected in [("", "TIMESTAMP"), (" WITH TIME ZONE", "TIMESTAMPTZ"),
                                 (" WITH LOCAL TIME ZONE", "TIMESTAMPTZ")]:
            for precision in [""] + [f"({p})" for p in range(10)]:
                with self.subTest(precision=precision, suffix=suffix):
                    self.assertEqual(cast_oracle_to_postgresql_type("TIMESTAMP" + precision + suffix), expected)

    def test_case_and_whitespace(self):
        for value, expected in [("  TimeStamp ( 6 ) \t", "TIMESTAMP"),
                                ("timestamp\t(3)\nWith  Time\tZone", "TIMESTAMPTZ"),
                                ("TIMESTAMP ( 9 ) with LOCAL time zone", "TIMESTAMPTZ")]:
            with self.subTest(value=value):
                self.assertEqual(cast_oracle_to_postgresql_type(value), expected)

    def test_other_types_and_unknowns_keep_existing_contract(self):
        for value, expected in [("DATE", "TIMESTAMP"), ("NUMBER", "NUMERIC"),
                                ("VARCHAR2", "VARCHAR"), ("BLOB", "BYTEA"),
                                (" binary_double ", "DOUBLE PRECISION"),
                                ("INTERVAL DAY TO SECOND", "INTERVAL")]:
            self.assertEqual(cast_oracle_to_postgresql_type(value), expected)
        for value in ["", "unknown", "NUMBER(10)", "VARCHAR2(20)", "TIMESTAMP()",
                      "TIMESTAMP(10)", "TIMESTAMP(-1)", "TIMESTAMP(1.5)",
                      "TIMESTAMP(6,2)", "TIMESTAMP(\u0666)", "TIMESTAMP(6)junk",
                      "TIMESTAMP(6) WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE(6)"]:
            with self.subTest(value=value):
                self.assertEqual(cast_oracle_to_postgresql_type(value), "TEXT")
        for value in [None, 6]:
            with self.assertRaises(AttributeError):
                cast_oracle_to_postgresql_type(value)

    def test_catalog_discovery_to_postgres_ddl_keeps_base_type_contract(self):
        oracle = OracleConnector("host", "user", "password", 1521, "db", "source")
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [
            (1, "PLAIN", "TIMESTAMP(6)", 11, "YES", None, "NO", "NO", "NO"),
            (2, "ZONED", "TIMESTAMP(3) WITH TIME ZONE", 13, "YES", None, "NO", "NO", "NO"),
            (3, "LOCAL", "TIMESTAMP(9) WITH LOCAL TIME ZONE", 11, "YES", None, "NO", "NO", "NO"),
        ]
        with patch.object(oracle, "_open_connection", return_value=connection):
            columns = oracle.extract_table_schema("events")
        self.assertEqual([c["type"] for c in columns], ["TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMPTZ"])
        self.assertEqual([c["length"] for c in columns], [11, 13, 11])
        pg = PostgresConnector("host", "user", "password", 5432, "db", "public")
        ddl, _ = pg.build_create_table_statement("events", "public", columns)
        self.assertEqual(ddl, 'CREATE TABLE IF NOT EXISTS "public"."events" (\n  "PLAIN" TIMESTAMP,\n  "ZONED" TIMESTAMPTZ,\n  "LOCAL" TIMESTAMPTZ\n);')
        cursor.close.assert_called_once()
        connection.close.assert_called_once()
