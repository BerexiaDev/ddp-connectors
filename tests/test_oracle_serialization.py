"""Oracle's existing serialization contract, without the application library."""
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
import unittest

import ddp_connectors
from ddp_connectors.database_connectors.oracle_connector import serialize_if_needed


class OracleSerializationTests(unittest.TestCase):
    def test_containers_keep_existing_json_format(self):
        cases = [
            ({}, "{}"),
            ([], "[]"),
            ({"name": "caf\u00e9", "items": [1, False, None]},
             '{"name": "caf\\u00e9", "items": [1, false, null]}'),
            ([{"value": 2}, "text"], '[{"value": 2}, "text"]'),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(serialize_if_needed(value), expected)

    def test_non_containers_are_returned_unchanged(self):
        for value in (None, False, True, 0, 2.5, "text", b"bytes",
                      Decimal("1.20"), date(2026, 1, 1), datetime(2026, 1, 1),
                      (1, 2), {1, 2}, object()):
            with self.subTest(value=value):
                self.assertIs(serialize_if_needed(value), value)

    def test_invalid_container_values_still_raise(self):
        with self.assertRaises(TypeError):
            serialize_if_needed({"value": object()})
        circular = []
        circular.append(circular)
        with self.assertRaises(ValueError):
            serialize_if_needed(circular)

    def test_oracle_factory_and_serializer_work_without_ddp_lib(self):
        # A fresh interpreter prevents a previously imported dependency hiding
        # the unwanted import. Use the same source/installed package as this test.
        script = """
import builtins
import sys
sys.path.insert(0, sys.argv[1])
original_import = builtins.__import__
def without_ddp_lib(name, *args, **kwargs):
    if name == "ddp_lib" or name.startswith("ddp_lib."):
        raise ModuleNotFoundError("ddp-lib must not be required")
    return original_import(name, *args, **kwargs)
builtins.__import__ = without_ddp_lib
from ddp_connectors.connectors_factory import ConnectorFactory
connector = ConnectorFactory().create_connector("oracle", {
    "host": "host", "user": "reader", "password": "test-only",
    "port": 1521, "database": "service", "schema": "reporting",
})
assert connector.schema == "REPORTING"
from ddp_connectors.database_connectors.oracle_connector import serialize_if_needed
assert serialize_if_needed({"value": [1]}) == '{"value": [1]}'
"""
        root = Path(ddp_connectors.__file__).resolve().parent.parent
        result = subprocess.run([sys.executable, "-I", "-c", script, str(root)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
