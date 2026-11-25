"""R03: startup parameters, per-instance isolation and canonical factory contracts."""

import os
import unittest
from unittest.mock import Mock, patch

import psycopg2

from ddp_connectors.connection_labels import normalize_application_name
from ddp_connectors.connectors_factory import ConnectorFactory
from ddp_connectors.database_connectors.postgres_connector import PostgresConnector

SETTINGS = dict(
    host="db.example.test",
    user="reader",
    password="test-secret",
    port=5440,
    database="warehouse",
    schema="public",
)
DRIVER = "ddp_connectors.database_connectors.postgres_connector.psycopg2.connect"


class ConnectionLabelTests(unittest.TestCase):
    def test_startup_retains_canonical_options_without_transaction(self):
        """A new PostgreSQL connection gets the cleaned label and the usual settings, without starting a transaction."""
        for schema, search_path in [
            ("public", "public"),
            ("Archive", "Archive,public"),
            ('"Archive"', "Archive,public"),
            (None, "public"),
        ]:
            with self.subTest(schema=schema), patch(DRIVER) as connect:
                pg = ConnectorFactory(" ddp-api:rule_preview ").create_connector(
                    "postgres", {**SETTINGS, "schema": schema}
                )
                conn = pg.get_connection()
                connect.assert_called_once_with(
                    host=SETTINGS["host"],
                    user=SETTINGS["user"],
                    password=SETTINGS["password"],
                    port=SETTINGS["port"],
                    dbname=SETTINGS["database"],
                    connect_timeout=10,
                    options=f"-c search_path={search_path}",
                    application_name="ddp-api:rule_preview",
                )
                conn.cursor.assert_not_called()
                conn.commit.assert_not_called()
                conn.rollback.assert_not_called()

    def test_interleaved_connectors_and_same_factory_remain_isolated(self):
        """Each connector keeps its own label when several connections are opened one after another."""
        factory = ConnectorFactory("ddp-api:source")
        a = factory.create_connector("postgres", SETTINGS)
        b = ConnectorFactory("ddp-sync:sync_full:opaque-id").create_connector(
            "postgres", SETTINGS
        )
        other = factory.create_connector("postgres", SETTINGS)
        with patch(DRIVER) as connect:
            for connector in (a, b, a, other):
                connector.get_connection()
            self.assertEqual(
                [call.kwargs["application_name"] for call in connect.call_args_list],
                [
                    a.application_name,
                    b.application_name,
                    a.application_name,
                    a.application_name,
                ],
            )
        self.assertNotIn("application_name", SETTINGS)

    def test_normalization_is_ascii_bounded_and_deterministic(self):
        """A label is trimmed to 63 plain characters, and an empty or invalid label falls back to a safe name."""
        cases = [
            (None, "ddp:source"),
            ("", "ddp:source"),
            (" \n\t ", "ddp:source"),
            (" ddp-api:préview\x00\x7f\nX ", "ddp-api:pr_view___X"),
            ("a" * 64, "a" * 63),
            ("界" * 70, "_" * 63),
            (" !~ ", "!~"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                value = normalize_application_name(raw)
                self.assertEqual(value, expected)
                self.assertLessEqual(len(value.encode("ascii")), 63)
                self.assertEqual(normalize_application_name(value), value)
                self.assertEqual(
                    ConnectorFactory(raw)
                    .create_connector("postgres", SETTINGS)
                    .application_name,
                    expected,
                )
        self.assertEqual(
            ConnectorFactory(
                "\t", default_application_name="ddp-sync:source"
            ).application_name,
            "ddp-sync:source",
        )
        self.assertEqual(normalize_application_name("", "é" * 70), "_" * 63)

    def test_direct_constructor_is_backwards_compatible_and_normalizes(self):
        """Building a PostgreSQL connector directly still works, and its label is cleaned the same way."""
        self.assertEqual(PostgresConnector(**SETTINGS).application_name, "ddp:source")
        self.assertEqual(
            PostgresConnector(**SETTINGS, application_name=" x\x00é ").application_name,
            "x__",
        )

    def test_retries_and_owned_helpers_use_the_same_label(self):
        """A failed connect that is tried again, and a helper connection, both use the same label."""
        pg = ConnectorFactory("ddp-sync:source_read").create_connector(
            "postgres",
            {**SETTINGS, "max_connection_retries": 1, "retry_backoff_seconds": 0},
        )
        conn = Mock()
        with patch(
            DRIVER, side_effect=[psycopg2.OperationalError("connection refused"), conn]
        ) as connect:
            self.assertTrue(
                pg.ping()
            )  # Inherited helper opens _open_connection directly.
            self.assertEqual(connect.call_count, 2)
            self.assertEqual(
                [c.kwargs["application_name"] for c in connect.call_args_list],
                [pg.application_name] * 2,
            )
        conn.close.assert_called_once()
        # Retry settings are still validated by the canonical factory.
        with self.assertRaises(ValueError):
            ConnectorFactory("ddp-api:source").create_connector(
                "postgres", {**SETTINGS, "max_connection_retries": -1}
            )

    def test_non_postgres_factories_keep_constructor_arguments(self):
        """Other database types are still built with their old arguments and do not receive this label."""
        cases = [
            ("sqlserver", "sql_server_connector", "SqlServerConnector", ()),
            (
                "informix",
                "informix_connector",
                "InformixConnector",
                ("onsoctcp", "en_US.utf8"),
            ),
            ("oracle", "oracle_connector", "OracleConnector", ("public",)),
            ("mysql", "mysql_connector", "MySQLConnector", ()),
            ("db2i", "db2i_connector", "Db2iConnector", ("public",)),
            ("mongo", "mongo_connector", "MongoConnector", ()),
        ]
        settings = {**SETTINGS, "protocol": "onsoctcp", "locale": "en_US.utf8"}
        original = dict(settings)
        for kind, module, cls, extra in cases:
            with (
                self.subTest(kind=kind),
                patch(f"ddp_connectors.database_connectors.{module}.{cls}") as ctor,
            ):
                result = ConnectorFactory("ddp-api:source").create_connector(
                    kind, settings
                )
                ctor.assert_called_once_with(
                    settings["host"],
                    settings["user"],
                    settings["password"],
                    settings["port"],
                    settings["database"],
                    *extra,
                )
                self.assertIs(result, ctor.return_value)
        self.assertEqual(settings, original)
        with patch(
            "ddp_connectors.file_connectors.sftp_connector.SftpConnector.from_settings"
        ) as create:
            self.assertIs(
                ConnectorFactory("ddp-api:source").create_connector("sftp", settings),
                create.return_value,
            )
            create.assert_called_once_with(settings)


@unittest.skipUnless(
    os.getenv("DDP_TEST_POSTGRES_DSN"),
    "Set DDP_TEST_POSTGRES_DSN to a disposable PostgreSQL",
)
class ConnectionLabelLiveTests(unittest.TestCase):
    def connector(self, label):
        """Build a real PostgreSQL connector from the test database address and the given label."""
        params = psycopg2.extensions.parse_dsn(os.environ["DDP_TEST_POSTGRES_DSN"])
        return PostgresConnector(
            params.get("host"),
            params.get("user"),
            params.get("password"),
            params.get("port"),
            params.get("dbname"),
            "public",
            application_name=label,
        )

    def test_rollback_helpers_and_physical_connection_isolation(self):
        """On a real database, undoing a change restores the original label and each open is a separate connection."""
        a, b = self.connector("ddp-api:rule_preview"), self.connector(
            "ddp-sync:sync_full:test"
        )
        opened = []
        real_connect = psycopg2.connect

        def connect(**kwargs):
            """Open a real connection and record its process id and label."""
            conn = real_connect(**kwargs)
            opened.append(
                (conn.get_backend_pid(), conn.get_parameter_status("application_name"))
            )
            # Reading protocol status needs no cursor and does not start a transaction.
            self.assertEqual(
                conn.get_transaction_status(),
                psycopg2.extensions.TRANSACTION_STATUS_IDLE,
            )
            return conn

        with patch(DRIVER, side_effect=connect):
            connections = [a.get_connection(), b.get_connection(), a.get_connection()]
            try:
                for conn, label in zip(
                    connections,
                    [a.application_name, b.application_name, a.application_name],
                ):
                    with conn.cursor() as cur:
                        cur.execute(
                            "SET application_name = 'temporary-transaction-name'"
                        )
                    conn.rollback()
                    with conn.cursor() as cur:
                        cur.execute("SHOW application_name")
                        self.assertEqual(cur.fetchone()[0], label)
                    conn.rollback()
                self.assertTrue(a.ping())
                a.get_connection_tables()  # Owned library helper opens and closes another physical connection.
                self.assertEqual(
                    [label for _, label in opened],
                    [a.application_name, b.application_name] + [a.application_name] * 3,
                )
                self.assertEqual(len(set(pid for pid, _ in opened)), 5)
                with connections[1].cursor() as cur:
                    cur.execute(
                        "SELECT application_name FROM pg_stat_activity WHERE pid = %s",
                        (connections[0].get_backend_pid(),),
                    )
                    self.assertEqual(cur.fetchone()[0], a.application_name)
            finally:
                for conn in connections:
                    conn.close()

    @unittest.skipUnless(
        os.getenv("DDP_TEST_POSTGRES_SSL") == "1",
        "Set DDP_TEST_POSTGRES_SSL=1 for a TLS server",
    )
    def test_libpq_ssl_environment_is_preserved(self):
        """Requiring a secure connection still connects and still keeps the label."""
        with patch.dict(os.environ, {"PGSSLMODE": "require"}):
            conn = self.connector("ddp-api:ssl_probe").get_connection()
            try:
                self.assertTrue(conn.info.ssl_in_use)
                self.assertEqual(
                    conn.get_parameter_status("application_name"), "ddp-api:ssl_probe"
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
