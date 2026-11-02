"""Opt-in Oracle discovery/value tests and PostgreSQL DDL execution.

DDP_TEST_ORACLE_SETTINGS is a factory settings JSON object. A uniquely named
Oracle table is created and dropped; PostgreSQL DDL is transactionally rolled back.
"""
from datetime import datetime
import json
import os
import unittest
import uuid

import oracledb
import psycopg2
from psycopg2 import sql

from ddp_connectors.connectors_factory import ConnectorFactory
from ddp_connectors.database_connectors.postgres_connector import PostgresConnector


@unittest.skipUnless(os.environ.get("DDP_TEST_ORACLE_SETTINGS"), "No Oracle configured")
class OracleLiveTests(unittest.TestCase):
    def setUp(self):
        self.oracle = ConnectorFactory().create_connector("oracle", json.loads(os.environ["DDP_TEST_ORACLE_SETTINGS"]))
        self.table = "DDPC_" + uuid.uuid4().hex[:20].upper()
        self.qualified_table = self.oracle._qualify_table_sql(self.table)
        self.connection = self.oracle.get_connection()
        self.addCleanup(self.connection.close)
        self.cursor = self.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute(f'CREATE TABLE {self.qualified_table} (T0 TIMESTAMP(0), T3 TIMESTAMP(3), T6 TIMESTAMP(6), T9 TIMESTAMP(9), TZ TIMESTAMP(3) WITH TIME ZONE, LTZ TIMESTAMP(6) WITH LOCAL TIME ZONE, D DATE)')
        self.addCleanup(self.cursor.execute, f'DROP TABLE {self.qualified_table} PURGE')

    def test_catalog_schema_and_fractional_values(self):
        self.cursor.execute("SELECT data_type FROM all_tab_columns WHERE owner=:1 AND table_name=:2 ORDER BY column_id", [self.oracle.schema, self.table])
        self.assertEqual([r[0] for r in self.cursor.fetchall()], ["TIMESTAMP(0)", "TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP(6) WITH LOCAL TIME ZONE", "DATE"])
        columns = self.oracle.extract_table_schema(self.table)
        self.assertEqual([c["type"] for c in columns], ["TIMESTAMP"] * 4 + ["TIMESTAMPTZ", "TIMESTAMPTZ", "TIMESTAMP"])
        self.cursor.setinputsizes(oracledb.DB_TYPE_TIMESTAMP)
        self.cursor.execute(f'INSERT INTO {self.qualified_table} (T6, T9) VALUES (:1, TO_TIMESTAMP(\'2026-11-02 12:34:56.123456789\', \'YYYY-MM-DD HH24:MI:SS.FF9\'))', [datetime(2026, 11, 2, 12, 34, 56, 654321)])
        self.connection.commit()
        rows = self.oracle.fetch_batch(self.cursor, self.table, 0, 1)
        self.assertEqual(rows[0][2], datetime(2026, 11, 2, 12, 34, 56, 654321))
        # Python datetime has microsecond resolution: no nanosecond claim.
        self.assertEqual(rows[0][3], datetime(2026, 11, 2, 12, 34, 56, 123456))
        self.assertIn("654321", self.oracle.extract_data_batch(self.table)[0]["T6"])

    @unittest.skipUnless(os.environ.get("DDP_TEST_POSTGRES_DSN"), "No test PostgreSQL configured")
    def test_discovered_schema_executes_on_postgres_and_preserves_microseconds(self):
        columns = self.oracle.extract_table_schema(self.table)
        pg = PostgresConnector("unused", "unused", "unused", 5432, "unused", "public")
        schema = "ddpc_" + uuid.uuid4().hex
        connection = psycopg2.connect(os.environ["DDP_TEST_POSTGRES_DSN"])
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                ddl, _ = pg.build_create_table_statement(self.table, schema, columns)
                cursor.execute(ddl)
                cursor.execute("SELECT data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", [schema, self.table])
                self.assertEqual([r[0] for r in cursor.fetchall()], ["timestamp without time zone"] * 4 + ["timestamp with time zone"] * 2 + ["timestamp without time zone"])
                value = datetime(2026, 11, 2, 12, 34, 56, 654321)
                self.cursor.setinputsizes(oracledb.DB_TYPE_TIMESTAMP)
                self.cursor.execute(f'INSERT INTO {self.qualified_table} (T6) VALUES (:1)', [value])
                row = self.oracle.fetch_batch(self.cursor, self.table, 0, 1)[0]
                cursor.execute(sql.SQL('INSERT INTO {}.{} ("T6") VALUES (%s) RETURNING "T6"').format(sql.Identifier(schema), sql.Identifier(self.table)), [row[2]])
                self.assertEqual(cursor.fetchone()[0], value)
        finally:
            connection.rollback()
            connection.close()
