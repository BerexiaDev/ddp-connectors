"""Logging contracts: real connectors, fake driver IO, no live databases."""
import json
import unittest
from unittest.mock import Mock, patch

import pyodbc
from loguru import logger

from ddp_connectors.connectors_factory import ConnectorFactory
from ddp_connectors.database_connectors.informix_connector import InformixConnector
from ddp_connectors.database_connectors.postgres_connector import PostgresConnector


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.messages = []
        self.records = []
        def collect(message):
            self.messages.append(str(message))
            self.records.append(message.record)
        sink = logger.add(collect, format='{message}', diagnose=True)
        self.addCleanup(logger.remove, sink)
        self.settings = dict(host='bank.internal', port=9088, database='bank',
                             user='bank_reader', password='private;pass}',
                             protocol='onsoctcp', locale='en_US.819', schema='reporting')

    def connector(self, kind='informix'):
        return ConnectorFactory().create_connector(kind, self.settings)

    def events(self):
        return [json.loads(r['message'].split(' ', 1)[1]) for r in self.records
                if r['message'].startswith('connector_error ')]

    def test_informix_preserves_driver_details_without_raw_credentials(self):
        connector = self.connector()
        error = pyodbc.Error('7000', '[7000] [DataDirect][ODBC 20101 driver]6015 (0) '
                            '(SQLDriverConnect) UID=bank_reader;PWD={private;pass}}};')
        with patch.object(connector, '_open_connection', side_effect=error) as connect:
            self.assertFalse(connector.ping())
        connect.assert_called_once()
        output = ''.join(self.messages)
        self.assertNotIn('bank_reader', output)
        self.assertNotIn('private;pass', output)
        details = connector.last_error_details
        self.assertEqual(details['reported_sqlstate'], '7000')
        self.assertEqual(details['odbc_operation'], 'SQLDriverConnect')
        self.assertIn('6015', details['error'])
        self.assertEqual(details['connector']['host'], 'bank.internal')
        self.assertEqual(details['connector']['locale'], 'en_US.819')
        self.assertIn('Could not connect to the Informix server', connector.last_error)
        self.assertEqual(self.events(), [details])
        self.assertTrue(all(r['exception'] is None for r in self.records))

    def test_all_database_pings_redact_and_clear_diagnostics_after_success(self):
        for kind in ('informix', 'sqlserver', 'postgres', 'oracle', 'mysql', 'db2i', 'mongo'):
            with self.subTest(kind=kind):
                connector = self.connector(kind)
                self.assertIsNone(getattr(connector, 'last_error_details', None))
                with patch.object(connector, '_open_connection', side_effect=RuntimeError('rejected bank_reader private;pass}')):
                    self.assertFalse(connector.ping())
                self.assertNotIn('private;pass', connector.last_error)
                self.assertNotIn('bank_reader', connector.last_error)
                self.assertEqual(connector.last_error_details['exception_type'], 'RuntimeError')
                connection = Mock()
                connection.cursor.return_value.fetchone.return_value = (1,)
                with patch.object(connector, '_open_connection', return_value=connection):
                    self.assertTrue(connector.ping())
                self.assertIsNone(connector.last_error)
                self.assertIsNone(connector.last_error_details)

    def test_read_failure_is_logged_without_changing_exception_or_retry_budget(self):
        connector = self.connector()
        connector.max_connection_retries = 1
        connector.retry_backoff_seconds = 0
        error = pyodbc.OperationalError('08S01', 'link failed private;pass}')
        with patch.object(connector, '_open_connection', side_effect=error) as connect:
            with self.assertRaises(pyodbc.OperationalError) as caught:
                connector._run_read_with_retry('get_connection_schemas', lambda cursor: None)
        self.assertIs(caught.exception, error)
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(self.events()[0]['operation'], 'get_connection_schemas')
        self.assertNotIn('private;pass', ''.join(self.messages))

    def test_postgres_metadata_failure_has_no_locals_or_credentials(self):
        connector = self.connector('postgres')
        local_only_secret = 'do-not-log-local-variables'
        try:
            raise RuntimeError('UID=bank_reader;PWD={private;pass}}};')
        except RuntimeError as error:
            connector._log_metadata_failure('discover', 'catalog', error, schema='reporting')
        output = ''.join(self.messages)
        self.assertNotIn(local_only_secret, output)
        self.assertNotIn('bank_reader', output)
        self.assertNotIn('private;pass', output)
        self.assertTrue(all(r['exception'] is None for r in self.records))
        self.assertEqual(self.events()[0]['operation'], 'discover')
        self.assertIn('test_postgres_metadata_failure', self.events()[0]['traceback'])

    def test_query_error_logging_is_redacted_and_keeps_rollback(self):
        connector = self.connector('postgres')
        connection = Mock()
        connection.cursor.return_value.execute.side_effect = RuntimeError('rejected private;pass}')
        with patch.object(connector, 'get_connection', return_value=connection):
            connector.create_schema_if_missing('reporting')
        connection.rollback.assert_called_once()
        self.assertNotIn('private;pass', ''.join(self.messages))
        self.assertEqual(len(self.events()), 1)

    def test_postgres_debug_snapshot_does_not_dump_session_users_or_rows(self):
        connector = self.connector('postgres')
        cursor = Mock()
        cursor.fetchall.return_value = [('bank_reader', 'session-private-value')]
        connector._log_debug_query(cursor, 'session_context', 'SELECT current_user')
        output = ''.join(self.messages)
        self.assertNotIn('bank_reader', output)
        self.assertNotIn('session-private-value', output)
        self.assertIn('returned_count=1', output)

    def test_unknown_credentials_uri_and_multiline_messages_are_redacted(self):
        connector = self.connector()
        error = RuntimeError('PWD={unknown;password}}}; token="another secret"; '
                             'postgres://outside:uri-secret@host/db\nforged entry')
        with patch.object(connector, '_open_connection', side_effect=error):
            self.assertFalse(connector.ping())
        output = ''.join(self.messages) + connector.last_error
        for secret in ('unknown;password', 'another secret', 'uri-secret'):
            self.assertNotIn(secret, output)
        self.assertNotIn('\n', self.records[-1]['message'])

    def test_escaped_credentials_are_redacted_before_tuple_serialization(self):
        connector = self.connector()
        connector.user = r'DOMAIN\reader'
        connector.password = 'a\\b"c\'d'
        error = pyodbc.Error('28000', f'rejected {connector.user!r} {connector.password!r}')
        with patch.object(connector, '_open_connection', side_effect=error):
            self.assertFalse(connector.ping())
        output = ''.join(self.messages) + connector.last_error
        self.assertNotIn('DOMAIN', output)
        # Windows traceback paths can legitimately contain "AppData\\".
        self.assertNotIn('a\\', connector.last_error_details['error'] + connector.last_error)

    def test_short_username_cannot_disable_generic_credential_masking(self):
        connector = self.connector()
        for username in ('pass', 'postgres'):
            connector.user = username
            error = RuntimeError('password=outside-secret; postgres://external:uri-secret@host/db')
            with patch.object(connector, '_open_connection', side_effect=error):
                self.assertFalse(connector.ping())
            output = ''.join(self.messages) + connector.last_error
            self.assertNotIn('outside-secret', output)
            self.assertNotIn('uri-secret', output)

    def test_unknown_quoted_password_survives_tuple_escaping_without_leaking(self):
        connector = self.connector()
        error = pyodbc.Error('28000', 'PWD=\'unknown secret\'; Host="db.internal"')
        with patch.object(connector, '_open_connection', side_effect=error):
            self.assertFalse(connector.ping())
        output = ''.join(self.messages) + connector.last_error
        self.assertNotIn('unknown', output)
        self.assertNotIn('secret', output)
        self.assertEqual(connector.last_error_details['reported_sqlstate'], '28000')

    def test_generic_mask_cannot_split_a_known_password(self):
        connector = self.connector()
        for password in ('two words', 'abc;tail', 'key} ending', '\ud800'):
            with self.subTest(password=password):
                connector.password = password
                with patch.object(connector, '_open_connection',
                                  side_effect=RuntimeError(f'password={password}')):
                    self.assertFalse(connector.ping())
                self.assertEqual(connector.last_error, 'password=[REDACTED]')
                self.assertEqual(connector.last_error_details['error'], 'password=[REDACTED]')

    def test_tuple_diagnostics_omit_non_scalar_arguments(self):
        from ddp_connectors.diagnostics import log_failure
        connector = self.connector()
        for value in ({'private;pass}': 'rejected'}, ('private;pass}',), b'private;pass}'):
            details = log_failure(connector, 'ping', RuntimeError('28000', value))
            self.assertNotIn('private;pass', details['error'])

    def test_diagnostics_failure_does_not_replace_ping_failure(self):
        connector = self.connector()
        class BadError(Exception):
            def __str__(self):
                raise ValueError('private;pass}')
        with patch.object(connector, '_open_connection', side_effect=BadError()):
            self.assertFalse(connector.ping())
        self.assertNotIn('private;pass', ''.join(self.messages))
        self.assertIn('omitted', connector.last_error_details['error'])

    def test_missing_oracle_schema_has_diagnostics_and_no_stale_error(self):
        connector = self.connector('oracle')
        connection = Mock()
        connection.cursor.return_value.fetchone.return_value = (0,)
        with patch.object(connector, '_open_connection', return_value=connection):
            self.assertFalse(connector.ping())
        self.assertIn('REPORTING', connector.last_error)
        self.assertIn('REPORTING', connector.last_error_details['error'])


if __name__ == '__main__':
    unittest.main()
