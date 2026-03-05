# Connectors Library

A unified Python database abstraction layer that provides a consistent interface for connecting to, extracting from, and managing multiple database systems. Built for ETL/ELT pipelines, data migrations, and cross-database operations.

---

## What This Library Does

CMR Connectors Library sits between your application and your databases, offering a single API to interact with **PostgreSQL**, **SQL Server**, and **IBM Informix**. Instead of writing database-specific logic for each system, you use one factory to create connectors and one set of methods to work with any supported database.

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

All connectors inherit from a shared abstract class (`SqlConnector`) that defines the contract every connector must fulfill. This guarantees that regardless of the underlying database, the same set of operations is available.

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

| Parameter    | PostgreSQL | SQL Server | Informix | Description                        |
|-------------|:----------:|:----------:|:--------:|-------------------------------------|
| `host`      | Required   | Required   | Required | Database server hostname or IP      |
| `user`      | Required   | Required   | Required | Authentication username             |
| `password`  | Required   | Required   | Required | Authentication password             |
| `port`      | Required   | Required   | Required | Database server port                |
| `database`  | Required   | Required   | Required | Target database name                |
| `schema`    | Optional   | -          | -        | PostgreSQL schema (search_path)     |
| `protocol`  | -          | -          | Required | Informix connection protocol        |
| `locale`    | -          | -          | Optional | Informix client locale setting      |

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
        sql_connector_utils.py         # Type mapping and conversion utilities
        utils/
            enums.py                   # Query building enumerations
            postgres_connector_utils.py # PostgreSQL query builder utilities
```

---

## Dependencies

| Package      | Purpose                                      |
|-------------|----------------------------------------------|
| `pyodbc`    | SQL Server and Informix database connections  |
| `psycopg2`  | PostgreSQL database connections               |
| `sqlalchemy` | SQL type mapping and handling                |
| `cx_oracle`  | Oracle database connections (reserved)       |
| `loguru`     | Structured logging                           |

---

## Publishing to PyPI

1. Install build tools: `pip install wheel twine`
2. Build the distribution: `python setup.py sdist bdist_wheel`
3. Upload to PyPI: `twine upload dist/*`

---

## Requirements

- Python >= 3.6
- Appropriate database drivers installed on the host system (ODBC Driver 17 for SQL Server, Informix ODBC driver for Informix)

---

## License

MIT
