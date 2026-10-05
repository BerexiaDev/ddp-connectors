# Changelog

## 0.2.0 — ready for release, not published yet

- Put the package settings in `pyproject.toml`.
- Support CPython 3.10–3.12.
- List all required connector drivers, pandas, PyMongo and the shared ddp-lib package so they are installed automatically.
- Use oracledb instead of cx_oracle, matching the driver used by the code.
- Use psycopg2-binary instead of psycopg2 to avoid compiling the PostgreSQL driver during a normal installation.
- Add automatic checks that the package builds, installs and imports successfully,and that its required libraries are installed with compatible versions.
