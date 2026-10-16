import copy
import unittest
from unittest.mock import Mock, patch

from ddp_connectors.database_connectors.postgres_connector import PostgresConnector


def column(name, type="INTEGER", **values):
    return {"name": name, "type": type, "nullable": "NO", "default": None,
            "primary_key": "NO", "is_index": "NO", **values}


class PostgresDdlTests(unittest.TestCase):
    def setUp(self):
        self.pg = PostgresConnector("host", "user", "password", 5432, "db", "public")

    def test_literal_defaults_require_explicit_opt_in(self):
        cases = [("7", "7"), (0, "0"), (False, "FALSE"), ("true", "true"),
                 ("NULL", "NULL"), ("-1.25e2", "-1.25e2"),
                 ("''", "''"), ("'it''s ready'", "'it''s ready'"),
                 ("'caf\u00e9'", "'caf\u00e9'"),
                 ("'ready'::text", "'ready'::text"),
                 ("'2026-01-01'::date", "'2026-01-01'::date")]
        for value, literal in cases:
            with self.subTest(value=value):
                cols = [column("value", default=value)]
                before = copy.deepcopy(cols)
                legacy, _ = self.pg.build_create_table_statement("items", "public", cols)
                self.assertNotIn("DEFAULT", legacy)
                sql, _ = self.pg.build_create_table_statement("items", "public", cols, include_defaults=True)
                self.assertIn("DEFAULT " + literal, sql)
                self.assertEqual(cols, before)

    def test_defaults_never_replay_functions_or_source_dialect_expressions(self):
        for value in [None, "", "now()", "nextval('seq'::regclass)", "SYSDATE", "((0))",
                      "CURRENT_TIMESTAMP", "1; DROP TABLE items", "'x'::custom_type",
                      "'x' -- comment", "'unclosed", "'a\\b'", "1 + 2", "NaN", "Infinity",
                      "\u0661", "\uff11", "fal\u017fe"]:
            with self.subTest(value=value):
                sql, _ = self.pg.build_create_table_statement(
                    "items", "public", [column("value", default=value)], include_defaults=True)
                self.assertNotIn("DEFAULT", sql)

    def test_partition_has_one_explicit_composite_pk_and_keeps_input(self):
        for flags in [("NO", "NO"), ("YES", "NO"), ("YES", "YES")]:
            cols = [column("id", primary_key=flags[0]),
                    column("created_at", "TIMESTAMP", primary_key=flags[1])]
            before = copy.deepcopy(cols)
            sql, indexes = self.pg.build_create_partitioned_table_statement(
                "items", "public", cols, "created_at", "id", "RANGE")
            self.assertEqual(sql.count("PRIMARY KEY"), 1)
            self.assertIn('PRIMARY KEY ("id", "created_at")', sql)
            self.assertTrue(sql.endswith('PARTITION BY RANGE ("created_at");'))
            self.assertEqual(cols, before)
            self.assertIsNone(indexes)
            ordinary, _ = self.pg.build_create_table_statement("items", "public", cols)
            self.assertEqual(ordinary.count("PRIMARY KEY"), int("YES" in flags))

    def test_partition_default_opt_in_and_identical_key(self):
        cols = [column("id", primary_key="YES", default="7")]
        legacy, _ = self.pg.build_create_partitioned_table_statement("items", "public", cols, "id", "id")
        sql, _ = self.pg.build_create_partitioned_table_statement(
            "items", "public", cols, "id", "id", include_defaults=True)
        self.assertNotIn("DEFAULT", legacy)
        self.assertIn("DEFAULT 7", sql)
        self.assertIn('PRIMARY KEY ("id")', sql)
        self.assertEqual(sql.count("PRIMARY KEY"), 1)

    def test_partition_preserves_legacy_builder_override_signature(self):
        class CustomConnector(PostgresConnector):
            def build_create_table_statement(self, table_name, schema_name, columns):
                return super().build_create_table_statement(table_name, schema_name, columns)

        connector = CustomConnector("host", "user", "password", 5432, "db", "public")
        sql, _ = connector.build_create_partitioned_table_statement(
            "items", "public", [column("id"), column("created", "DATE")], "created", "id")
        self.assertIn('PRIMARY KEY ("id", "created")', sql)

    def test_ddl_identifiers_are_escaped_without_changing_exact_names(self):
        cols = [column(' Id"\\1 ', 'numeric(18,4)', primary_key="YES", is_index="YES"),
                column('When"Created', 'TIMESTAMP WITH TIME ZONE')]
        sql, index = self.pg.build_create_table_statement('Order"Items', 'Mixed Schema', cols)
        self.assertIn('"Mixed Schema"."Order""Items"', sql)
        self.assertIn('" Id""\\1 " numeric(18,4)', sql)
        self.assertIn('PRIMARY KEY (" Id""\\1 ")', sql)
        self.assertIn('ON "Mixed Schema"."Order""Items" (" Id""\\1 ")', index)
        partition, _ = self.pg.build_create_partitioned_table_statement(
            'Order"Items', 'Mixed Schema', cols, 'When"Created', ' Id"\\1 ')
        self.assertIn('PRIMARY KEY (" Id""\\1 ", "When""Created")', partition)
        self.assertIn('PARTITION BY RANGE ("When""Created")', partition)

    def test_default_and_range_partition_sql_escapes_names_and_rolls_over(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        with patch.object(self.pg, "get_connection", return_value=connection):
            self.pg.create_default_partition('Mi"xed', 'Ta"ble')
            self.assertEqual(cursor.execute.call_args.args[0],
                             'CREATE TABLE IF NOT EXISTS "Mi""xed"."ta""ble__p_default" PARTITION OF "Mi""xed"."Ta""ble" DEFAULT;')
            self.pg.create_range_partitions_year_month('Mi"xed', 'Ta"ble', 'At"Time', "month", ["2026-12"])
            self.assertEqual(cursor.execute.call_args.args[0],
                             'CREATE TABLE IF NOT EXISTS "Mi""xed"."ta""ble__p_at""time__2026_12" PARTITION OF "Mi""xed"."Ta""ble" FOR VALUES FROM (\'2026-12-01\') TO (\'2027-01-01\');')
            self.pg.create_range_partitions_year_month("public", "items", "created", "year", ["2026"])
            self.assertIn("FROM ('2026-01-01') TO ('2027-01-01')", cursor.execute.call_args.args[0])
        self.assertEqual(connection.commit.call_count, 3)
        self.assertEqual(cursor.close.call_count, 3)
        self.assertEqual(connection.close.call_count, 3)

    def test_partition_failure_rolls_back_and_propagates(self):
        connection = Mock()
        connection.cursor.return_value.execute.side_effect = RuntimeError("database failure")
        with patch.object(self.pg, "get_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "database failure"):
                self.pg.create_default_partition("public", "items")
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
