import importlib
from importlib.metadata import distribution
from pathlib import Path
import pkgutil
import unittest


# The version number we expect the installed package to have
EXPECTED_VERSION = "0.2.0"

class ImportSmokeTests(unittest.TestCase):

    # Test 1: check that the two Java-related libraries can be imported
    def test_lazily_loaded_dependencies_import(self):
        for name in ("jaydebeapi", "jpype"):
            with self.subTest(module=name):
                importlib.import_module(name)

    # Test 2: check that the installed package is correct and all its modules can be imported
    def test_all_shipped_modules_import_from_installed_distribution(self):
        # Import the main package
        package = importlib.import_module("ddp_connectors")

        # Get the information of the installed package (from pip)
        installed = distribution("ddp-connectors")

        # Check the installed version is the one we expect (EXPECTED_VERSION)
        self.assertEqual(installed.version, EXPECTED_VERSION)

        # Check the allowed Python versions are ">=3.10" and "<3.13"
        self.assertEqual(set(installed.metadata["Requires-Python"].split(",")),
                         {">=3.10", "<3.13"})

        # Check the package we imported is the same file as the installed one
        self.assertEqual(Path(package.__file__).resolve(),
                         Path(installed.locate_file("ddp_connectors/__init__.py")).resolve())

        # Check we are NOT importing from the project's source folder.
        self.assertNotIn(Path(__file__).resolve().parents[1], Path(package.__file__).parents)

        # Find every module inside the package (including sub-packages)
        modules = list(pkgutil.walk_packages(package.__path__, package.__name__ + "."))

        # Make sure we found at least one module
        self.assertTrue(modules)

        # Try to import each module, one by one
        for module in modules:
            with self.subTest(module=module.name):
                importlib.import_module(module.name)


if __name__ == "__main__":
    unittest.main()