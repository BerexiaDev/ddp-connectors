"""Optional real PostgreSQL regression; set DDP_TEST_POSTGRES_DSN to enable."""
import os
import unittest
import uuid
from unittest.mock import patch

import psycopg2
from psycopg2 import sql

from ddp_connectors.database_connectors.postgres_connector import PostgresConnector
from test_postgres_ddl import column


@unittest.skipUnless(os.environ.get("DDP_TEST_POSTGRES_DSN"), "No test PostgreSQL configured")
class PostgresLiveTests(unittest.TestCase):
    def test_partition_defaults_exact_identifiers_and_single_key_execute(self):
        dsn = os.environ["DDP_TEST_POSTGRES_DSN"]
        schema = 'kan5_' + uuid.uuid4().hex + '" exact'
        table, key, date = 'Events"Log', 'Id"Key', 'At"Date'
        connector = PostgresConnector("unused", "unused", "unused", 5432, "unused", schema)
        connection = psycopg2.connect(dsn)
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                ddl, indexes = connector.build_create_partitioned_table_statement(
                    table, schema,
                    [column(key, primary_key="YES", is_index="YES", default="7"),
                     column(date, "DATE", primary_key="YES", default="'2026-12-01'::date")],
                    date, key, include_defaults=True)
                cursor.execute(ddl)
                cursor.execute(indexes)
                with patch.object(connector, "get_connection", side_effect=lambda: psycopg2.connect(dsn)):
                    connector.create_range_partitions_year_month(schema, table, date, "month", ["2026-12"])
                    connector.create_default_partition(schema, table)
                cursor.execute(sql.SQL("INSERT INTO {}.{} DEFAULT VALUES RETURNING {}, {}")
                               .format(*map(sql.Identifier, [schema, table, key, date])))
                identity, created = cursor.fetchone()
                self.assertEqual(identity, 7)
                self.assertEqual(created.isoformat(), "2026-12-01")
                cursor.execute(
                    "SELECT count(*) FROM information_schema.table_constraints "
                    "WHERE table_schema = %s AND table_name = %s AND constraint_type = 'PRIMARY KEY'",
                    (schema, table))
                self.assertEqual(cursor.fetchone()[0], 1)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            connection.close()


if __name__ == "__main__":
    unittest.main()
