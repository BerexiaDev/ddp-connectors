# Release procedure

The proposed release is **ddp-connectors 0.2.0**. Existing version tags must remain
unchanged. The import namespace remains `ddp_connectors`; Python package indexes
normalize underscores and hyphens to the same distribution name.

1. Review the KAN-5 matrix in [docs/KAN-5.md](docs/KAN-5.md) and the changelog.
2. Use Python 3.10+ and install build tooling:
   `python -m pip install build twine`.
3. Run `python scripts/smoke_install.py` and repeat with
   `--constraints tests/consumer-constraints.txt` on Python 3.10.
4. Build clean artifacts with `python -m build`, then run
   `python -m twine check --strict dist/*`. Keep wheel/sdist hashes and smoke logs
   with the release record.
5. Release ddp-connectors 0.2.0 independently. It does not require a ddp-lib release,
   source checkout or package installation.
6. Tag the reviewed commit `0.2.0` and publish artifacts through the team's
   existing package release channel. Do not move earlier tags or publish donor
   packages. No publication is automated by this change.
7. KAN-8 updates consumer pins and runs service integration tests. Validate actual
   database drivers/services there; isolated import tests do not establish live
   database connectivity.

CI runs wheel/sdist smoke checks on Python 3.10, 3.11 and 3.12; the consumer
constraint profile is tested on 3.10, matching existing service images.

CI checks out only this repository using GitHub's built-in token with
`contents: read`. No additional secret, sibling checkout or dependency
reference input is required. The smoke suite verifies ddp-lib is not installed
and all connectors, including Oracle, import successfully.

Install system drivers separately: the ODBC runtime plus SQL Server/Informix
drivers, and a JVM plus the configured JTOpen jar for Db2 for i. SFTP, Oracle
thin mode, PostgreSQL, MySQL and MongoDB Python imports are included in package
dependency tests. The package cannot provision servers or vendor system drivers.
