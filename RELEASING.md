# Release procedure

The Oracle/Informix fixes in **ddp-connectors 0.2.2** are documented in
[the verification and adoption record](docs/CONNECTOR-FIXES-20261102.md).
Existing version tags must remain unchanged. The import namespace remains
`ddp_connectors`; Python package indexes
normalize underscores and hyphens to the same distribution name.

1. Review the KAN-5 matrix in [docs/KAN-5.md](docs/KAN-5.md) and the changelog.
2. Use Python 3.10+ and install build tooling:
   `python -m pip install build twine`.
3. Run `python scripts/smoke_install.py` and repeat with
   `--constraints tests/consumer-constraints.txt` on Python 3.10.
4. Build clean artifacts with `python -m build`, then run
   `python -m twine check --strict dist/*`. Keep wheel/sdist hashes and smoke logs
   with the release record.
5. Release ddp-connectors 0.2.2 independently. It does not require a ddp-lib release,
   source checkout or package installation.
6. Verify `0.2.2` is absent both locally (`git tag --list 0.2.2`) and remotely
   (`git ls-remote --tags origin refs/tags/0.2.2 'refs/tags/0.2.2^{}'`). Commit
   the reviewed release files and push the current branch to origin. Create
   the annotated tag with `git tag -a 0.2.2 -m "Release ddp-connectors 0.2.2"`
   and push it with `git push origin refs/tags/0.2.2`. Never force-push or replace
   an existing tag. Confirm the remote peeled tag `refs/tags/0.2.2^{}` equals
   the release commit SHA. Core installs from Git, so this tag is the release
   delivery mechanism; uploading to a package index is a separate action.
7. KAN-8 updates consumer pins and runs service integration tests. Validate actual
   database drivers/services there; isolated import tests do not establish live
   database connectivity.

CI runs wheel/sdist smoke checks on Python 3.10, 3.11 and 3.12; the consumer
constraint profile is tested on 3.10, matching existing service images.
For checks that must leave existing database data untouched, point
`DDP_TEST_POSTGRES_DSN` at a disposable PostgreSQL instance and leave Oracle
and Informix live-test settings unset. The original real-driver results and
their limitations are recorded in the verification document.

CI checks out only this repository using GitHub's built-in token with
`contents: read`. No additional secret, sibling checkout or dependency
reference input is required. The smoke suite verifies ddp-lib is not installed
and all connectors, including Oracle, import successfully.

Install system drivers separately: the ODBC runtime plus SQL Server/Informix
drivers, and a JVM plus the configured JTOpen jar for Db2 for i. SFTP, Oracle
thin mode, PostgreSQL, MySQL and MongoDB Python imports are included in package
dependency tests. The package cannot provision servers or vendor system drivers.
