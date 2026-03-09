import re
from typing import Dict

import sqlalchemy


def safe_convert_to_string(value):
    """
    Safely convert a value to a string, handling UTF-8 encoding issues.
    
    Args:
        value: The value to convert
        
    Returns:
        str or None: The string representation of the value, or None if the value is None
    """
    if value is None:
        return None

    try:
        # Handle bytes objects by decoding them with UTF-8
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')

        # For other types, convert to string
        return str(value)
    except Exception as e:
        return None


def cast_sql_to_typescript_types(sa_type):
    # String types
    if isinstance(sa_type, (sqlalchemy.String, sqlalchemy.Unicode, sqlalchemy.Text,
                            sqlalchemy.UnicodeText, sqlalchemy.CHAR, sqlalchemy.VARCHAR)):
        return "string"
    # Number types
    if isinstance(sa_type, (sqlalchemy.Integer, sqlalchemy.BigInteger,
                            sqlalchemy.SmallInteger, sqlalchemy.Float, sqlalchemy.Numeric)):
        return "number"
    # Boolean type
    if isinstance(sa_type, sqlalchemy.Boolean):
        return "boolean"
    # Date types
    if isinstance(sa_type, (sqlalchemy.Date, sqlalchemy.DateTime, sqlalchemy.TIMESTAMP)):
        return "Date"
    if isinstance(sa_type, sqlalchemy.Time):
        return "string"  # convert to string for now
    # Array types
    if isinstance(sa_type, sqlalchemy.ARRAY):
        inner_type = cast_sql_to_typescript_types(sa_type.item_type)
        return f"{inner_type}[]"
    # Enum types
    if isinstance(sa_type, sqlalchemy.Enum):
        return "string"
    # Default fallback
    return "string"


def cast_informix_to_typescript_types(informix_type: int) -> str:
    """Maps Informix coltype to Typescript types."""

    informix_to_ts = {
        # Basic numeric types
        1: "number",  # SMALLINT
        2: "number",  # INTEGER
        3: "number",  # FLOAT
        4: "number",  # SMALLFLOAT
        5: "number",  # DECIMAL
        6: "number",  # SERIAL (Auto-increment INT)
        8: "number",  # MONEY
        17: "number",  # INT8 (BIGINT)
        18: "number",  # SERIAL8 (Auto-increment BIGINT)
        52: "number",  # BIGINT
        53: "number",  # BIGSERIAL (Auto-increment BIGINT)
        25: "number",  # REFSERIAL
        26: "number",  # REFSERIAL8
        262: "number",  # DISTINCT type (numeric based)

        # String types
        0: "string",  # CHAR
        12: "string",  # TEXT (Large character object)
        13: "string",  # VARCHAR
        15: "string",  # NCHAR (Fixed-length Unicode)
        16: "string",  # NVARCHAR (Variable-length Unicode)
        40: "string",  # LVARCHAR (Large variable-length string)
        42: "string",  # CLOB (Character large object)
        43: "string",  # LVARCHAR (Client-side only)
        27: "string",  # LVARCHAR (alternate variant)
        35: "string",  # IDSXML
        37: "string",  # IDSCHARSET
        256: "string",  # IDSXML
        258: "string",  # IDSXML
        269: "string",  # VARCHAR with NOT NULL
        2061: "string",  # IDSSECURITYLABEL (security label string)

        # Date and time types
        7: "Date",  # DATE
        10: "Date",  # DATETIME
        14: "string",  # INTERVAL (Duration, might need parsing)
        263: "Date",  # DATE

        # Boolean types
        41: "boolean",  # BOOLEAN (newer Informix versions)
        45: "boolean",  # BOOLEAN
        28: "boolean",  # BOOLEAN (alias/variant)
        32: "boolean",  # BOOLEAN (older versions)

        # Binary types
        11: "binary",  # BYTE (Binary data)
        31: "binary",  # BLOB
        36: "binary",  # IDSBLOB

        # Collection types
        19: "string[]",  # SET (Unordered collection)
        20: "string[]",  # MULTISET (May contain duplicates)
        21: "string[]",  # LIST (Ordered collection)
        23: "any[]",  # COLLECTION (General collection type)

        # Record/Composite types
        22: "Record<string, any>",  # ROW (Unnamed composite type)
        24: "Record<string, any>",  # ROW (opaque UDT)
        4117: "Record<string, any>",  # ROW (opaque composite)
        4118: "Record<string, any>",  # ROW (Named composite type)

        # Special types
        9: "null",  # NULL (unspecified type)
    }

    return informix_to_ts.get(informix_type, "unknown")  # Default to "unknown" if type is not listed


def cast_informix_to_postgresql_type(informix_type: int) -> str:
    """Maps Informix coltype (MOD(coltype, 256)) to PostgreSQL data types."""
    informix_to_pg = {
        # Numeric types
        1: "SMALLINT",  # SMALLINT
        2: "INTEGER",  # INTEGER
        3: "DOUBLE PRECISION",  # FLOAT
        4: "REAL",  # SMALLFLOAT
        5: "DECIMAL",  # DECIMAL(p,s)
        6: "SERIAL",  # SERIAL (Auto-increment)
        8: "NUMERIC",  # MONEY
        17: "BIGINT",  # INT8
        18: "BIGSERIAL",  # SERIAL8
        52: "BIGINT",  # BIGINT
        53: "BIGSERIAL",  # BIGSERIAL
        25: "INTEGER",  # REFSERIAL
        26: "BIGINT",  # REFSERIAL8
        262: "INTEGER",  # DISTINCT type based on INT

        # Character/String types
        0: "CHAR",  # CHAR(n)
        12: "TEXT",  # TEXT
        13: "VARCHAR",  # VARCHAR(n)
        15: "CHAR",  # NCHAR
        16: "VARCHAR",  # NVARCHAR
        40: "VARCHAR",  # LVARCHAR
        42: "TEXT",  # CLOB
        43: "VARCHAR",  # LVARCHAR client-side only
        27: "VARCHAR",  # LVARCHAR variant
        35: "TEXT",  # IDSXML
        37: "TEXT",  # IDSCHARSET
        256: "TEXT",  # IDSXML variant
        258: "TEXT",  # IDSXML variant
        269: "VARCHAR",  # VARCHAR NOT NULL
        2061: "TEXT",  # IDSSECURITYLABEL

        # Date/Time types
        7: "DATE",  # DATE
        10: "TIMESTAMP",  # DATETIME
        14: "INTERVAL",  # INTERVAL
        263: "DATE",  # DATE (variant)

        # Boolean types
        41: "BOOLEAN",  # BOOLEAN
        45: "BOOLEAN",  # BOOLEAN
        28: "BOOLEAN",  # BOOLEAN
        32: "BOOLEAN",  # BOOLEAN

        # Binary types
        11: "BYTEA",  # BYTE (binary)
        31: "BYTEA",  # BLOB
        36: "BYTEA",  # IDSBLOB

        # Collections
        19: "TEXT[]",  # SET
        20: "TEXT[]",  # MULTISET
        21: "TEXT[]",  # LIST
        23: "JSONB",  # COLLECTION (could vary)

        # Composite types
        22: "JSONB",  # ROW (unnamed)
        24: "JSONB",  # ROW (opaque UDT)
        4117: "JSONB",  # ROW (opaque composite)
        4118: "JSONB",  # ROW (named composite)

        # Special
        9: "TEXT"  # NULL / unspecified
    }

    base_type = informix_type % 256
    return informix_to_pg.get(base_type, "TEXT")



def cast_postgres_to_typescript(data_type: str) -> str:
    """
    Simple mapping from Postgres data_type/udt_name to a TS type.
    Extend this as needed.
    """
    data_type = data_type.lower().strip()
    mapping: Dict[str, str] = {
        # numeric types
        "smallint": "number",
        "integer": "number",
        "bigint": "number",
        "numeric": "number",
        "decimal": "number",
        "real": "number",
        "smallserial": "number",
        "serial": "number",
        "bigserial": "number",
        "money": "string",
        "double precision": "number",

        # character types
        "character varying": "string",
        "varchar": "string",
        "character": "string",
        "char": "string",
        "text": "string",
        "citext": "string",

        # boolean
        "boolean": "boolean",
        "bool": "boolean",

        # date/time
        "date": "Date",
        "time": "string",
        "time without time zone": "string",
        "time with time zone": "string",
        "timestamp": "Datetime",
        "timestamp without time zone": "Datetime",
        "timestamp with time zone":    "Datetime",
        "interval":                 "string",

        "json": "any",
        "jsonb": "any",
        "uuid": "string",
    }

    if data_type == "USER-DEFINED":
        return "string"

    return mapping.get(data_type, "any")


def cast_sqlserver_to_typescript_types(sql_type: str) -> str:
    """
       Convert an SQL-Server column type to a TypeScript-friendly type
       using a direct dictionary lookup.

       Unknown or unlisted SQL types fall back to `'any'`.
    """

    sql_type = sql_type.lower().strip()

    sql_server_to_ts: Dict[str, str] = {
        # numeric
        "int": "number",
        "bigint": "number",
        "smallint": "number",
        "tinyint": "number",
        "decimal": "number",
        "numeric": "number",
        "float": "number",
        "real": "number",
        "money": "number",
        "smallmoney": "number",

        # boolean
        "bit": "boolean",

        # textual
        "char": "string",
        "nchar": "string",
        "varchar": "string",
        "nvarchar": "string",
        "text": "string",
        "ntext": "string",
        "xml": "string",
        "uniqueidentifier": "string",
        "sysname": "string",

        # binary / blob ───────
        "binary": "string",
        "varbinary": "string",
        "image": "string",
        "rowversion": "string",
        "timestamp": "string",

        # temporal
        "date": "Date",
        "time": "string",
        "datetime": "Datetime",
        "datetime2": "string",
        "smalldatetime": "Datetime",
        "datetimeoffset": "string",

        # special / spatial
        "hierarchyid": "any",
        "geography": "any",
        "geometry": "any",
        "sql_variant": "any",
    }

    return sql_server_to_ts.get(sql_type, "any")


def cast_oracle_to_typescript(oracle_type: str) -> str:
    """
    Maps an Oracle data type to a corresponding TypeScript type.
    Handles precision/scale modifiers and complex timestamp declarations.
    """
    if not oracle_type:
        return "any"

    raw_type = oracle_type.lower().strip()

    # Strip out any modifiers in parentheses (e.g., "(255)", "(10, 2)", "(6)") (timestamp(6) => timestamp)
    base_type = re.sub(r'\s*\([^)]*\)', '', raw_type).strip()

    mapping: Dict[str, str] = {
        # Numeric Types -> TS number
        "number": "number",
        "float": "number",
        "binary_float": "number",
        "binary_double": "number",
        "int": "number",
        "integer": "number",
        "smallint": "number",
        "decimal": "number",
        "dec": "number",
        "numeric": "number",

        # Character / String Types -> TS string
        "varchar2": "string",
        "nvarchar2": "string",
        "varchar": "string",
        "char": "string",
        "nchar": "string",
        "clob": "string",
        "nclob": "string",
        "long": "string",
        "rowid": "string",
        "urowid": "string",

        # Date & Time Types -> TS Date
        "date": "Date",
        "timestamp": "Date",
        "timestamp with time zone": "Date",
        "timestamp with local time zone": "Date",
        "interval year to month": "string", # Intervals are usually parsed as strings formatted as ISO durations
        "interval day to second": "string",

        # Binary / BLOB Types -> TS Uint8Array (Standard for binary data in JS/TS)
        "blob": "List",
        "raw": "List",
        "long raw": "List",
        "bfile": "List",

        # Misc Types
        "xmltype": "string",
        "json": "any",      # Can also be Record<string, any> depending on your TS strictness
        "boolean": "boolean" # Oracle 23c introduced native BOOLEAN
    }

    # 4. Return the mapped type, falling back to "any" if unknown
    return mapping.get(base_type, "any")

def cast_sqlserver_to_postgresql_type(sql_server_type: str) -> str:
    """
    Map SQL-Server type to Postgres type.
    :param sql_server_type:
    :return:
        postgres type
    """
    sql_server_to_pg: Dict[str, str] = {

        # Numerics
        "int": "INTEGER",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "tinyint": "SMALLINT",  # no 1-byte integer in PG
        "decimal": "NUMERIC",
        "numeric": "NUMERIC",
        "money": "MONEY",
        "smallmoney": "MONEY",
        "float": "DOUBLE PRECISION",
        "real": "REAL",

        # Boolean
        "bit": "BOOLEAN",

        # Character / Text
        "char": "CHAR",
        "nchar": "CHAR",
        "varchar": "VARCHAR",
        "nvarchar": "VARCHAR",
        "text": "TEXT",
        "ntext": "TEXT",
        "xml": "XML",

        # Binary / BLOB
        "binary": "BYTEA",
        "varbinary": "BYTEA",
        "image": "BYTEA",
        "rowversion": "BYTEA",
        "timestamp": "BYTEA",

        # Misc Scalars
        "uniqueidentifier": "UUID",
        "sql_variant": "JSONB",
        "sysname": "TEXT",

        # Temporal
        "date": "DATE",
        "time": "TIME",
        "datetime": "TIMESTAMP",
        "smalldatetime": "TIMESTAMP",
        "datetime2": "TIMESTAMP",
        "datetimeoffset": "TIMESTAMPTZ",

        # Spatial & Hierarchy (PostGIS / contrib types)
        "geometry": "GEOMETRY",
        "geography": "GEOGRAPHY",
        "hierarchyid": "LTREE",
    }

    return sql_server_to_pg.get(sql_server_type, "TEXT")


def cast_oracle_to_postgresql_type(oracle_type: str) -> str:
    """
    Map Oracle type to Postgres type.
    :param oracle_type: The Oracle data type as a string
    :return: The corresponding PostgreSQL data type
    """
    # Normalize the input to lowercase to ensure matching works regardless of input casing
    oracle_type_normalized = oracle_type.lower().strip()

    oracle_to_pg: Dict[str, str] = {

        # Numerics
        "number": "NUMERIC",
        "int": "INTEGER",
        "integer": "INTEGER",
        "smallint": "SMALLINT",
        "float": "DOUBLE PRECISION",
        "binary_float": "REAL",
        "binary_double": "DOUBLE PRECISION",
        "dec": "NUMERIC",
        "decimal": "NUMERIC",
        "double precision": "DOUBLE PRECISION",
        "real": "REAL",

        # Character / Text
        "char": "CHAR",
        "nchar": "CHAR",
        "varchar2": "VARCHAR",
        "nvarchar2": "VARCHAR",
        "varchar": "VARCHAR",
        "clob": "TEXT",
        "nclob": "TEXT",
        "long": "TEXT",

        # Binary / BLOB
        "blob": "BYTEA",
        "raw": "BYTEA",
        "long raw": "BYTEA",
        "bfile": "TEXT",  # BFILE stores a locator to a physical file; TEXT or VARCHAR is standard

        # Date & Time
        # Oracle's DATE includes time, so TIMESTAMP is the safest equivalent to prevent data loss
        "date": "TIMESTAMP", 
        "timestamp": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamp with local time zone": "TIMESTAMPTZ",
        "interval year to month": "INTERVAL",
        "interval day to second": "INTERVAL",

        # Misc
        "xmltype": "XML",
        "rowid": "TEXT",  # Typically mapped to text or varchar in PG
        "urowid": "TEXT",
        "json": "JSONB",
    }

    # Default to TEXT if the type is not found
    return oracle_to_pg.get(oracle_type_normalized, "TEXT")