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

    oracle_type_normalized = " ".join(oracle_type.lower().strip().split())

    oracle_to_ts: Dict[str, str] = {
        # --- Numbers ---
        "number": "number",
        "numeric": "number",
        "dec": "number",
        "decimal": "number",
        "int": "number",
        "integer": "number",
        "smallint": "number",
        "float": "number",
        "real": "number",
        "double precision": "number",
        "binary_float": "number",
        "binary_double": "number",

        # --- Strings & Characters ---
        "char": "string",
        "character": "string",
        "nchar": "string",
        "national char": "string",
        "national character": "string",
        "varchar": "string",
        "varchar2": "string",
        "nvarchar2": "string",
        "character varying": "string",
        "char varying": "string",
        "national character varying": "string",
        "national char varying": "string",
        "clob": "string",
        "nclob": "string",
        "long": "string",
        "rowid": "string",
        "urowid": "string",
        "xmltype": "string",
        "bfile": "string",
        "sdo_geometry": "string", 
        "uritype": "string",
        "httpuritype": "string",
        "xdburitype": "string",
        "dburitype": "string",
        "ref": "string",
        "interval year to month": "string", # Intervals are usually parsed as ISO strings in JS
        "interval day to second": "string",

        # --- Dates & Times ---
        "date": "Date",
        "timestamp": "Datetime",
        "timestamp with time zone": "Datetime",
        "timestamp with local time zone": "Datetime",

        # --- Booleans ---
        "boolean": "boolean",
        "bool": "boolean",

        # --- Objects & Records ---
        "json": "record",
        "object": "record",

        # --- Lists & Arrays ---
        "vector": "list",
        "varray": "list",
        "nested table": "list",

        # --- Binaries & Unknowns ---
        # BLOB/RAW usually come through the API as base64 strings or ArrayBuffers, 
        # but 'any' is the safest fallback for the frontend if you don't have a specific BLOB enum.
        "raw": "any",
        "long raw": "any",
        "blob": "any",
        "anydata": "any",
        "anytype": "any",
        "anydataset": "any",
    }

    # Default to 'any' if the type is completely unrecognized
    return oracle_to_ts.get(oracle_type_normalized, "any")

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
    oracle_type_normalized = " ".join(oracle_type.lower().strip().split())

    oracle_to_pg: Dict[str, str] = {
        "number": "NUMERIC",
        "numeric": "NUMERIC",
        "dec": "NUMERIC",
        "decimal": "NUMERIC",

        "int": "INTEGER",
        "integer": "INTEGER",
        "smallint": "SMALLINT",

        "float": "DOUBLE PRECISION",
        "real": "REAL",
        "double precision": "DOUBLE PRECISION",

        "binary_float": "REAL",
        "binary_double": "DOUBLE PRECISION",

        "char": "CHAR",
        "character": "CHAR",

        "nchar": "CHAR",
        "national char": "CHAR",
        "national character": "CHAR",

        "varchar": "VARCHAR",
        "varchar2": "VARCHAR",

        "nvarchar2": "VARCHAR",
        "character varying": "VARCHAR",
        "char varying": "VARCHAR",
        "national character varying": "VARCHAR",
        "national char varying": "VARCHAR",

        "clob": "TEXT",
        "nclob": "TEXT",
        "long": "TEXT",


        "raw": "BYTEA",
        "long raw": "BYTEA",
        "blob": "BYTEA",

        "bfile": "TEXT",

        "date": "TIMESTAMP",

        "timestamp": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamp with local time zone": "TIMESTAMPTZ",

        "interval year to month": "INTERVAL",
        "interval day to second": "INTERVAL",

        "rowid": "TEXT",
        "urowid": "TEXT",


        "xmltype": "XML",

        "json": "JSONB",

        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",

        "vector": "VECTOR",

        "ref": "TEXT",
        "object": "JSONB",
        "varray": "JSONB",
        "nested table": "JSONB",

        "sdo_geometry": "TEXT", 
        "uritype": "TEXT",
        "httpuritype": "TEXT",
        "xdburitype": "TEXT",
        "dburitype": "TEXT",

        "anydata": "JSONB",
        "anytype": "JSONB",
        "anydataset": "JSONB",
    }

    return oracle_to_pg.get(oracle_type_normalized, "TEXT")

def map_postgres_type(pg_type: str, target_db: str = "postgres") -> str:
    """
    Convert a PostgreSQL type string to the equivalent type
    for the target database: postgres, oracle, or informix.
    """
    if not pg_type:
        return pg_type

    original_type = pg_type.strip()
    target_db = (target_db or "postgres").strip().lower()

    if target_db in ("postgres", "postgresql"):
        return original_type.upper()

    # -----------------------------
    # Parse PostgreSQL type safely
    # -----------------------------
    clean_type = original_type.lower().strip()

    is_array = clean_type.endswith("[]")
    if is_array:
        clean_type = clean_type[:-2].strip()

    args = None
    paren_match = re.search(r"\(([^)]+)\)", clean_type)
    if paren_match:
        args = paren_match.group(1).strip()
        clean_type = re.sub(r"\([^)]+\)", "", clean_type).strip()

    base_type = re.sub(r"\s+", " ", clean_type).strip()

    # Normalize aliases
    if base_type in ("character varying",):
        base_type = "varchar"
    elif base_type in ("character",):
        base_type = "char"
    elif base_type == "timestamp without time zone":
        base_type = "timestamp"
    elif base_type == "timestamp with time zone":
        base_type = "timestamptz"
    elif base_type == "time without time zone":
        base_type = "time"
    elif base_type == "time with time zone":
        base_type = "timetz"

    # -----------------------------
    # Oracle mapping
    # -----------------------------
    if target_db == "oracle":
        if is_array:
            return "CLOB"

        if base_type == "varchar":
            return f"VARCHAR2({args})" if args else "VARCHAR2(4000)"
        if base_type == "char":
            return f"CHAR({args})" if args else "CHAR"
        if base_type in ("numeric", "decimal"):
            return f"NUMBER({args})" if args else "NUMBER"
        if base_type in ("bit", "bit varying"):
            return f"RAW({args})" if args else "RAW(2000)"

        oracle_map = {
            "smallint": "NUMBER(5)",
            "integer": "NUMBER(10)",
            "int": "NUMBER(10)",
            "bigint": "NUMBER(19)",
            "real": "BINARY_FLOAT",
            "double precision": "BINARY_DOUBLE",
            "money": "NUMBER(19,4)",
            "serial": "NUMBER(10)",
            "bigserial": "NUMBER(19)",
            "text": "CLOB",
            "bytea": "BLOB",
            "timestamp": "TIMESTAMP",
            "timestamptz": "TIMESTAMP WITH TIME ZONE",
            "date": "DATE",
            "time": "VARCHAR2(15)",
            "timetz": "VARCHAR2(20)",
            "interval": "INTERVAL DAY TO SECOND",
            "boolean": "NUMBER(1)",
            "uuid": "VARCHAR2(36)",
            "json": "CLOB",
            "jsonb": "CLOB",
            "xml": "XMLTYPE",
            "inet": "VARCHAR2(64)",
            "cidr": "VARCHAR2(64)",
            "macaddr": "VARCHAR2(32)",
        }

        return oracle_map.get(base_type, "VARCHAR2(4000)")

    # -----------------------------
    # Informix mapping
    # -----------------------------
    if target_db == "informix":
        if is_array:
            return "LVARCHAR"

        if base_type == "varchar":
            if args:
                return f"LVARCHAR({args})" if args.isdigit() and int(args) > 255 else f"VARCHAR({args})"
            return "LVARCHAR"

        if base_type == "char":
            return f"CHAR({args})" if args else "CHAR"

        if base_type in ("numeric", "decimal"):
            return f"DECIMAL({args})" if args else "DECIMAL"

        if base_type in ("bit", "bit varying"):
            return f"BYTE({args})" if args else "BYTE"

        informix_map = {
            "smallint": "SMALLINT",
            "integer": "INTEGER",
            "int": "INTEGER",
            "bigint": "BIGINT",
            "real": "SMALLFLOAT",
            "double precision": "FLOAT",
            "money": "MONEY",
            "serial": "SERIAL",
            "bigserial": "BIGSERIAL",
            "text": "TEXT",
            "bytea": "BLOB",
            "timestamp": "DATETIME YEAR TO FRACTION(5)",
            "timestamptz": "DATETIME YEAR TO FRACTION(5)",
            "date": "DATE",
            "time": "DATETIME HOUR TO SECOND",
            "timetz": "DATETIME HOUR TO SECOND",
            "interval": "INTERVAL",
            "boolean": "BOOLEAN",
            "uuid": "CHAR(36)",
            "json": "LVARCHAR",
            "jsonb": "LVARCHAR",
            "xml": "TEXT",
            "inet": "LVARCHAR(64)",
            "cidr": "LVARCHAR(64)",
            "macaddr": "LVARCHAR(32)",
        }

        return informix_map.get(base_type, "LVARCHAR")
        
    return original_type.upper()