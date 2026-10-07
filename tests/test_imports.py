import importlib
import importlib.util
from importlib.metadata import distribution, PackageNotFoundError
import os
from pathlib import Path
import pkgutil
import unittest


class ImportSmokeTests(unittest.TestCase):
    def test_every_shipped_module_and_lazy_driver_imports(self):
        package = importlib.import_module("ddp_connectors")
        modules = list(pkgutil.walk_packages(package.__path__, package.__name__ + "."))
        self.assertTrue(modules)
        for name in [module.name for module in modules] + ["jaydebeapi", "jpype"]:
            with self.subTest(module=name):
                importlib.import_module(name)
        if os.environ.get("DDP_INSTALL_SMOKE") == "1":
            installed = distribution("ddp-connectors")
            self.assertEqual(installed.version, "0.2.0")
            self.assertEqual(Path(package.__file__).resolve(),
                             Path(installed.locate_file("ddp_connectors/__init__.py")).resolve())
            self.assertNotIn(Path(__file__).resolve().parents[1], Path(package.__file__).parents)
            with self.assertRaises(PackageNotFoundError):
                distribution("ddp-lib")
            self.assertIsNone(importlib.util.find_spec("ddp_lib"))


if __name__ == "__main__":
    unittest.main()
