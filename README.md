# DDP Connectors Library

A unified Python database abstraction layer that provides a consistent interface for connecting to, extracting from, and managing multiple database systems. Built for ETL/ELT pipelines, data migrations, and cross-database operations.

---

## What This Library Does

DDP Connectors Library sits between your application and your databases, offering a single API to interact with **PostgreSQL**, **SQL Server**, **IBM Informix**, **Oracle**, **MySQL**, **MongoDB**, and **Db2 for IBM i**, plus read-only SFTP sources. Instead of writing database-specific logic for each system, you use one factory to create connectors and one set of methods to work with any supported database.

The library is designed around these core goals:

- **Extract data** from any supported database using a unified interface
- **Inspect and convert schemas** across different database type systems
- **Manage database structures** (tables, indexes, primary keys, partitions)
- **Build complex queries** from structured JSON specifications
- **Track data changes** through CDC (Change Data Capture) mechanisms

---

## Architecture

### Factory Pattern

The library uses a factory to instantiate the right connector based on the database type. You pass a type identifier (`postgres`, `sqlserver`, or `informix`) along with connection settings, and the factory returns a ready-to-use connector.

### Abstract Base Class

SQL connectors inherit from a shared abstract class (`SqlConnector`) that defines the contract every connector must fulfill. This guarantees that regardless of the underlying database, the same set of operations is available.

### Database-Specific Implementations

Each connector handles the dialect differences internally — pagination syntax, system catalog queries, type mappings, and connection drivers — so the consuming application doesn't need to care about those details.

---

## Supported Databases

### PostgreSQL

- **Driver:** psycopg2
- **Schema-aware:** Supports `search_path` for multi-schema environments
- **Pagination:** Standard `OFFSET` / `LIMIT`
- **Streaming:** Server-side cursors for memory-efficient large dataset processing
- **Filtering:** JSON-based filter specifications with parameterized queries (prevents SQL injection)
- **Partitioning:** Supports creating partitioned tables with RANGE, LIST, and HASH strategies
- **Query Builder:** Full complex query building from JSON — joins, aggregations, GROUP BY, HAVING, and advanced WHERE conditions

### SQL Server

- **Driver:** pyodbc (ODBC Driver 17)
- **Pagination:** `OFFSET...FETCH NEXT` syntax
- **Streaming:** Cursor-based batch streaming for large tables
- **Schema Extraction:** Queries `sys.indexes`, `sys.columns`, and `sys.foreign_key_columns` for full metadata
- **Type Conversion:** Automatically maps SQL Server types to PostgreSQL equivalents (useful for migrations)
- **Deduplication:** Handles duplicate column names during schema extraction

### IBM Informix

- **Driver:** pyodbc with Informix-specific driver path
- **Pagination:** `SKIP...FIRST` syntax
- **Locale Support:** Configurable CLIENT_LOCALE and DB_LOCALE
- **Encoding:** Handles Latin-1 (ISO-8859-1) for SQL_CHAR and UTF-8 for SQL_WCHAR
- **Type Mapping:** Translates Informix integer-based column type codes to both TypeScript and PostgreSQL types

### Db2 for IBM i

- **Driver:** `jaydebeapi` with the open-source **JTOpen JDBC driver** (`jt400.jar`) — Db2 for i / Db2/400 on IBM i / AS400. Chosen over the IBM i Access ODBC Driver because `jt400.jar` is freely downloadable (Maven Central / SourceForge) and **not** subject to US export-control gating.
- **Connection:** JDBC URL `jdbc:as400://<host>/<database>`; the "schema" is an IBM i *library* (passed as the `libraries` URL property). The `jt400.jar` path is read from the `JT400_JAR` env var (set in the service Dockerfiles).
- **Pagination:** `OFFSET...FETCH FIRST` syntax
- **Catalog Discovery:** Reads table/column/key metadata from the `QSYS2` system catalog views (`SYSTABLES`, `SYSCOLUMNS`, `SYSCST`, `SYSKEYCST`, `SYSINDEXES`, `SYSKEYS`)
- **Type Mapping:** Translates Db2-for-i `DATA_TYPE` names to both TypeScript and PostgreSQL types
- **Runtime requirement:** A JVM must be available (the service images already install Java)

---

## Read-only SFTP file sources

`sftp` is a file-oriented connector, not a database connector. It supports verified-host-key authentication, file/directory listing, metadata retrieval, and incremental binary reads only. It has no SQL/table API and cannot upload, delete, rename, or modify remote files.

Use either a `known_hosts` file or a pinned `SHA256:` host-key fingerprint; omitting both is rejected. Password authentication is supported, as is a private key supplied through `private_key_path` (with optional `private_key_passphrase`). SSH agent, keyboard-interactive, bastion, proxy, and remote-shell support are intentionally not provided.

```python
from ddp_connectors.connectors_factory import ConnectorFactory

connector = ConnectorFactory().create_connector("sftp", {
    "host": "sftp.example.org",
    "port": 22,
    "user": "acaps-reader",
    "password": "obtained-from-a-secret-store",
    "remote_root": "/exports/acaps",
    "known_hosts": "/run/secrets/sftp_known_hosts",
    "connect_timeout": 10,
    "auth_timeout": 10,
    "socket_timeout": 30,
})
try:
    for item in connector.list_files(recursive=True):
        if item.is_file:
            with connector.open_file(item.path) as source:
                while chunk := source.read(1024 * 1024):
                    # DeepKube owns the destination object-store upload.
                    consume(chunk)
finally:
    connector.close()
```

The connector is synchronous and must not be shared between threads. Streams are caller-owned; closing the connector invalidates any remaining open streams. Paths are constrained to `remote_root`; symlinks are returned by listing but never traversed or opened.

Configuration: `host`, `user`, and absolute `remote_root` are required; `port` defaults to 22. Configure exactly one authentication field (`password` or `private_key_path`) and exactly one host-trust field (`known_hosts` or `host_key_fingerprint`). Optional timeouts are `connect_timeout`, `auth_timeout`, and `socket_timeout`.

---

## Core Capabilities

### Data Extraction

- **Batch Pagination** — Fetch rows in configurable page sizes with offset-based pagination, each database using its native syntax
- **Streaming** — Process entire tables through server-side cursors without loading everything into memory, yielding batches as generators
- **Filtered Extraction** — Apply filters at the database level using JSON filter objects that support: equality, comparison (`>`, `<`, `>=`, `<=`), range (`BETWEEN`), set membership (`IN`), string matching (`CONTAINS`, `STARTS_WITH`, `ENDS_WITH`), regex (`MATCHES`), and null checks
- **Delta / CDC** — Fetch only changed rows from log tables since a given timestamp, enabling incremental data synchronization

### Schema Discovery and Conversion

- **Table Discovery** — List all user tables in a database or schema
- **Column Discovery** — Retrieve column names, data types, nullability, default values, and constraint information
- **Full Schema Extraction** — Get complete metadata including ordinal position, primary keys, foreign keys, indexes, and uniqueness
- **Cross-Database Type Mapping** — Convert native types between database systems:
  - SQL Server to PostgreSQL
  - Informix to PostgreSQL
  - Any SQL type to TypeScript (for API/frontend integration)
- **Data-Driven Type Detection** — Infer appropriate PostgreSQL types from pandas Series data samples

### Schema and Table Management

- **Schema Creation** — Create schemas if they don't already exist
- **Table Creation** — Generate and execute CREATE TABLE statements from extracted schemas, with optional index creation
- **Partitioned Tables** — Create partitioned tables with composite primary keys, supporting RANGE partitions by year/month and default partitions
- **Index Management** — Create or drop indexes on specified columns
- **Primary Key Management** — Create or drop primary key constraints
- **Truncation** — Clear all data from a table while preserving its structure

### Query Building (PostgreSQL)

The PostgreSQL connector includes an advanced query builder that constructs SQL from JSON specifications:

- **SELECT** — Fields with optional aggregations (COUNT, SUM, AVG, MIN, MAX, DISTINCT), aliases, and type casting
- **JOIN** — INNER, LEFT, and RIGHT joins with condition specifications
- **WHERE** — Multi-condition filtering with value comparisons, column-to-column comparisons, date extraction, and logical operators
- **GROUP BY** — Grouping with field references
- **HAVING** — Post-aggregation filtering on COUNT, SUM, and other aggregate functions

---

## Connection Settings

Each connector requires a settings dictionary with connection parameters:

| Parameter    | PostgreSQL | SQL Server | Informix | Db2 for i | Description                        |
|-------------|:----------:|:----------:|:--------:|:---------:|-------------------------------------|
| `host`      | Required   | Required   | Required | Required  | DB server hostname/IP (IBM i system name for Db2 for i) |
| `user`      | Required   | Required   | Required | Required  | Authentication username             |
| `password`  | Required   | Required   | Required | Required  | Authentication password             |
| `port`      | Required   | Required   | Required | Required  | Database server port (JDBC portNumber for Db2 for i) |
| `database`  | Required   | Required   | Required | Required  | Target database name (RDB name for Db2 for i) |
| `schema`    | Optional   | -          | -        | Optional  | PostgreSQL schema (search_path) / Db2-for-i library |
| `protocol`  | -          | -          | Required | -         | Informix connection protocol        |
| `locale`    | -          | -          | Optional | -         | Informix client locale setting      |

Use connector `type` = `db2i` for Db2 for IBM i.

---

## Project Structure

```
ddp_connectors/
    connectors_factory.py              # Factory for creating database connectors
    database_connectors/
        sql_connector.py               # Abstract base class (interface contract)
        postgres_connector.py          # PostgreSQL implementation
        sql_server_connector.py        # SQL Server implementation
        informix_connector.py          # IBM Informix implementation
        db2i_connector.py              # Db2 for IBM i implementation
        sql_connector_utils.py         # Type mapping and conversion utilities
        utils/
            enums.py                   # Query building enumerations
            postgres_connector_utils.py # PostgreSQL query builder utilities
```

---

## Installation and release

The next release is **0.2.0**, with Git tag **0.2.0**. Package information and
dependency ranges are in `pyproject.toml`. See [release steps](RELEASING.md).
The import name stays `ddp_connectors`.

After both GitHub tags are published, install the matching versions:

```sh
python -m pip install \
  'ddp-lib @ git+https://github.com/BerexiaDev/ddp-lib.git@0.2.0' \
  'ddp-connectors @ git+https://github.com/BerexiaDev/ddp-connectors.git@0.2.0'
python -m pip check
```

You need both links, because GitHub is not a Python package index. The
`ddp-lib` version must fit the range that `ddp-connectors` asks for
(`ddp-lib>=0.2.0,<0.3`). Git must be installed. Private repositories need Git
credentials with read access.

Before the tags exist, install both from your local folders:

```sh
python -m pip install ./ddp-lib ./ddp-connectors
```

`ddp-lib` is used for Oracle serialization. Do not install other packages that
use the same Python names (`ddp_connectors`, `ddp_lib`) in the same environment.
A normal install includes the Python driver for every connector, so you do not
need extras.

## Supported runtime

| Use case | Python | Install |
| --- | --- | --- |
| API, workers, sync | CPython 3.10–3.12 | ddp-connectors 0.2.x + ddp-lib 0.2.x |
| Authentication only | CPython 3.10–3.12 | ddp-lib 0.2.x only |

- Python below 3.10 and 3.13 or newer are not supported.
- The import tests need the ODBC manager. They do not need a database, a
  password, Java, or vendor drivers.
- Real connections need the extra items in the table below.

| Python package | Used for | Also needed to connect |
| --- | --- | --- |
| `psycopg2-binary` | PostgreSQL | A PostgreSQL server |
| `pyodbc` | SQL Server, Informix | unixODBC (`libodbc2` on Debian/Ubuntu) and the Microsoft ODBC Driver 17 or IBM Informix ODBC driver |
| `oracledb` | Oracle | Nothing extra (thin mode) |
| `mysql-connector-python` | MySQL | A MySQL server |
| `pymongo` | MongoDB | A MongoDB server. Never install the separate `bson` package |
| `JayDeBeApi`, `JPype1` | Db2 for IBM i | Java (JVM) and `jt400.jar`. Set `JT400_JAR` |
| `paramiko` | SFTP | An SFTP server and trusted host keys |
| `pandas` | PostgreSQL type detection | pandas 2.2 or newer (below 3) |
| `SQLAlchemy`, `loguru` | SQL types, logging | Nothing |
| `ddp-lib` | Oracle serialization | Nothing (no Flask setup needed) |

Notes:
- `psycopg2-binary` replaces `psycopg2`, so no compiler or `pg_config` is
  needed. Never install both together. The binary package contains its own
  client libraries, which only change when the package is updated. If you need
  system libraries, keep your own tested build. See the
  [Psycopg installation guide](https://www.psycopg.org/docs/install.html).
- `oracledb` replaces the wrong `cx_oracle` requirement, because `oracledb` is
  the library the code really imports.
- Servers, drivers and Java for real connections are the responsibility of the
  deployment.

## Upgrading an application

- Replace old Git links such as `ddp-connectors@0.0.4` with the `0.2.0` tag.
- Update pandas 1.5 and NumPy 1.23 together for pandas 2.2 or newer, and update
  Flask-RESTX 1.2 to 1.3.x for ddp-lib 0.2.
- Keep Core's PyMongo 3 setup. Both shared packages require `pymongo>=3.10.1,<4`;
  the existing `pymongo==3.10.1` pin remains allowed.
- The smoke tests do not prove that an existing application lock file works.
  Test the application itself.

## Verification

```sh
python -m pip install build twine

# Uses the sibling ../ddp-lib folder if it exists
python scripts/smoke_install.py

# ddp-lib in another folder
python scripts/smoke_install.py --dependency /path/to/ddp-lib

# ddp-lib from a GitHub tag (use this before a release)
python scripts/smoke_install.py --published-dependency 0.2.0
```

The script:
1. Builds the sdist and the wheel.
2. Checks the package information.
3. Installs each file in its own clean virtual environment.
4. Runs `pip check`.
5. Imports every module from outside the project folder, including the
   Java-related libraries.

It starts no Java and makes no database or SFTP connections. Live tests must
run where the package is used.

With `--published-dependency TAG`, the sibling folder is ignored and `ddp-lib`
is built from that tag. Other libraries still come from the normal package
index. If there is no sibling folder, you must choose `--dependency` or
`--published-dependency`. The script prints the chosen source first.


## License

MIT
