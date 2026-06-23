
from collections import Counter, defaultdict
from datetime import datetime
from itertools import islice
import json
import uuid
from loguru import logger
from typing import Dict, Any, List, Tuple
from pymongo import MongoClient

from bson import ObjectId
from bson.decimal128 import Decimal128
from bson.codec_options import TypeCodec, TypeRegistry, CodecOptions
from decimal import Decimal

# Maps the normalized BSON/JSON types produced by schema inference onto the
# coarse-grained column types the rest of the platform's UI / rule engine speaks.
PLATFORM_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "date": "date",
    "object": "record",
    "array": "list",
    "objectId": "string",
    "binary": "string",
    "uuid": "string",
    "null": "string",
}

# Maps inferred field types onto destination (PostgreSQL data-mart) column types.
# Nested objects/arrays are stored as JSONB; everything else lands in a typed column.
POSTGRES_TYPE_MAP = {
    "string": "TEXT",
    "integer": "BIGINT",
    "double": "DOUBLE PRECISION",
    "decimal": "NUMERIC",
    "boolean": "BOOLEAN",
    "date": "TIMESTAMP",
    "objectId": "TEXT",
    "binary": "BYTEA",
    "uuid": "UUID",
    "object": "JSONB",
    "array": "JSONB",
    "null": "TEXT",
}

# Column that captures any top-level field NOT present in the sampled schema, so a
# rare/late-appearing field is never silently dropped during sync (schema-drift safety).
EXTRA_COLUMN = "_extra"

class DecimalCodec(TypeCodec):
    python_type = Decimal    # When PyMongo sees a Python Decimal
    bson_type = Decimal128   # it should convert it to a Mongo Decimal128

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        return value.to_decimal()

type_registry = TypeRegistry([DecimalCodec()])
codec_options = CodecOptions(type_registry=type_registry)

class MongoConnector:

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database_name = database
        self.driver = "mongodb"
        # Caches the inferred relational schema per collection so that
        # extract_table_schema() (used to build the table DDL + INSERT columns) and
        # stream_batch()/fetch_batch() (which must yield rows in the SAME column order)
        # never disagree within a single sync run.
        self._relational_schema_cache: Dict[str, List[Dict[str, Any]]] = {}

    def ping(self) -> bool:
        """
        Tests the database connection.
        Returns True if successful, raises an Exception or returns False if not.
        """
        try:
            client = self.get_connection()
            # The 'ping' command is cheap and verifies the server is reachable
            client.admin.command('ping')
            logger.info(f"Successfully pinged MongoDB database: {self.database_name}")
            return True
        except Exception as exc:
            logger.error(f"MongoDB connection ping failed: {exc}")
            return False
        finally:
            if 'client' in locals():
                client.close()
    
    def get_connection(self) -> MongoClient:
        """
        Returns a MongoClient instance.
        """
        if self.user and self.password:
            # URL encode credentials if they contain special characters in a real-world scenario
            uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}?authSource=admin"
        else:
            uri = f"mongodb://{self.host}:{self.port}/{self.database_name}"
            
        return MongoClient(uri)

    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """
        Inserts a list of dictionaries into the specified MongoDB collection using insert_many.
        """
        if not data:
            logger.warning(f"No data provided to insert into {table_name}.")
            return 0

        client = self.get_connection()
        total_inserted = 0
        
        try:
            db = client[self.database_name]
            collection = db.get_collection(table_name, codec_options=codec_options)

            # ordered=False allows MongoDB to continue inserting the batch even if one document fails (ex., duplicate _id)
            result = collection.insert_many(data, ordered=False)
            total_inserted += len(result.inserted_ids)
            return total_inserted
            
        except Exception as exc:
            logger.error(f"Error inserting data into {table_name}: {exc}")
            raise
            
        finally:
            client.close()

    def fetch_batch(self, table_name: str, offset: int, limit: int = 100, **kwargs: Any):
        """
        Fetches a batch of documents using skip and limit.
        """
        client = self.get_connection()
        try:
            db = client[self.database_name]
            schema = self._relational_schema(table_name)
            cursor = db[table_name].find().skip(offset).limit(limit)
            return [self._project_document(doc, schema) for doc in cursor]
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []
        finally:
            client.close()

    def stream_batch(self, table_name: str, batch_size: int = 10_000):
        """
        Streams a collection as batches of row tuples ready for Postgres insertion.

        Each document is projected onto the promoted-column schema from
        ``extract_table_schema`` (top-level fields as typed columns, nested data as JSONB,
        unmodeled fields in ``_extra``) so the tuples line up positionally with the
        INSERT column list the sync pipeline builds.
        """
        client = self.get_connection()
        try:
            logger.info(f"Start streaming MongoDB collection {table_name} with batch_size={batch_size}")
            db = client[self.database_name]
            schema = self._relational_schema(table_name)

            # batch_size tells the MongoDB driver how many documents to fetch per network round trip
            cursor = db[table_name].find(batch_size=batch_size)

            while True:
                # 1. Grab exactly batch_size items directly from the cursor
                chunk = list(islice(cursor, batch_size))

                # If the chunk is empty, we've reached the end of the collection
                if not chunk:
                    break
                else:
                    yield [self._project_document(doc, schema) for doc in chunk]

            logger.info(f"Finished streaming MongoDB collection {table_name}")
        except Exception as exc:
            logger.error(f"Error streaming batch from MongoDB collection {table_name}: {exc}")
        finally:
            client.close()

    def get_connection_tables(self) -> List[str]:
        """Returns a list of collections in the database."""
        client = self.get_connection()
        try:
            db = client[self.database_name]
            collections = db.list_collection_names()
            logger.info(f"Collections: {collections}")
            return collections
        except Exception as e:
            logger.error(f"Error getting collections: {e}")
            return []
        finally:
            client.close()

    # Documents collapsed into the (id, data) storage contract used by the sync
    # pipeline. Returned as a safe fallback whenever live schema inference can't run.
    _FALLBACK_COLUMNS = [
        {"name": "id", "type": "string", "alias": "id"},
        {"name": "data", "type": "record", "alias": "data"},
    ]

    def get_connection_columns(self, table_name: str, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """
        MongoDB is schema-less, so there is no fixed column list. This samples documents
        and returns the *top-level* fields actually observed in the collection, each
        annotated with its presence rate and type distribution.

        Because the same collection can hold heterogeneous documents (field exists in
        document A but not B, or has different types), every column carries:
          - presence_pct : % of sampled docs in which the field is present at all
          - nullable     : True if the field is ever absent or null
          - polymorphic  : True if the field appears with more than one type
          - types        : the full observed type distribution

        Falls back to the (id, data) storage contract if the collection is empty or
        sampling fails, so existing callers keep working.
        """
        try:
            catalog = self.profile_collection_schema(table_name, sample_size=sample_size)
            fields = catalog.get("fields", [])

            columns = []
            for field in fields:
                path = field["path"]
                # Only surface top-level scalar/record fields as columns; nested and
                # array-element paths (containing "." or "[]") live inside the catalog.
                if "." in path or "[]" in path:
                    continue
                columns.append({
                    "name": path,
                    "alias": path,
                    "type": PLATFORM_TYPE_MAP.get(field["dominant_type"], "string"),
                    "presence_pct": field["presence_pct"],
                    "nullable": field["nullable"],
                    "polymorphic": field["polymorphic"],
                    "types": field["types"],
                })

            return columns or list(self._FALLBACK_COLUMNS)
        except Exception as e:
            logger.error(f"Error inferring columns for {table_name}, using fallback: {e}")
            return list(self._FALLBACK_COLUMNS)

    def profile_collection_schema(self, table_name: str, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Best-practice probabilistic schema discovery for a schema-less collection.

        Randomly samples up to ``sample_size`` documents (via ``$sample`` so the result
        is representative, not just the first N inserts) and builds a *field catalog*:
        every field path observed anywhere in the collection — including nested object
        paths (``user.address.city``) and array-element paths (``orders[]``) — together
        with how often it is present, whether it is ever null/absent, and the full
        distribution of types it takes.

        This is the primitive the field picker, profiler and quality-rule suggester all
        build on. It is intentionally read-only and does not change how data is synced.

        Returns::

            {
              "collection": "<name>",
              "sampled": <int>,            # number of documents actually sampled
              "fields": [ { ...catalog entry... }, ... ]
            }
        """
        client = self.get_connection()
        try:
            db = client[self.database_name]
            collection = db[table_name]
            try:
                docs = list(collection.aggregate(
                    [{"$sample": {"size": sample_size}}], allowDiskUse=True
                ))
            except Exception as exc:
                # $sample needs a non-empty collection / certain storage engines; fall
                # back to a plain bounded scan.
                logger.warning(f"$sample failed for {table_name} ({exc}); falling back to find().limit()")
                docs = list(collection.find().limit(sample_size))

            fields = self._infer_field_catalog(docs)
            logger.info(f"Inferred {len(fields)} field paths from {len(docs)} sampled docs in {table_name}")
            return {"collection": table_name, "sampled": len(docs), "fields": fields}
        finally:
            client.close()

    def _infer_field_catalog(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Pure inference over a list of sampled documents -> field catalog.

        Kept free of any network/IO so it can be unit-tested directly and reasoned about.
        Presence is counted once per document (so a field inside a 50-element array still
        counts as "present in 1 doc"), while the type distribution counts every occurrence
        (so type polymorphism inside arrays is still detected).
        """
        total = len(docs)
        type_counts: Dict[str, Counter] = defaultdict(Counter)
        null_counts: Dict[str, int] = defaultdict(int)
        present_docs: Dict[str, int] = defaultdict(int)
        examples: Dict[str, list] = defaultdict(list)

        for doc in docs:
            seen: set = set()
            self._walk_document(doc, "", seen, type_counts, null_counts, examples)
            for path in seen:
                present_docs[path] += 1

        fields = []
        for path in sorted(type_counts.keys()):
            counts = type_counts[path]
            occurrences = sum(counts.values())
            present = present_docs[path]
            distribution = [
                {"type": t, "pct": round(c * 100 / occurrences, 1)}
                for t, c in counts.most_common()
            ]
            fields.append({
                "path": path,
                "presence_pct": round(present * 100 / total, 1) if total else 0.0,
                "null_pct": round(null_counts[path] * 100 / total, 1) if total else 0.0,
                # Absent in some docs OR explicitly null => not a guaranteed field.
                "nullable": present < total or null_counts[path] > 0,
                "dominant_type": counts.most_common(1)[0][0],
                "polymorphic": len([t for t in counts if t != "null"]) > 1,
                "types": distribution,
                "examples": examples[path][:3],
            })
        return fields

    def _walk_document(self, value, path, seen, type_counts, null_counts, examples) -> None:
        """Recursively record the type/presence of every field path within a document."""
        if path != "":
            type_counts[path][self._normalize_type(value)] += 1
            seen.add(path)
            if value is None:
                null_counts[path] += 1

        if isinstance(value, dict):
            for key, child_value in value.items():
                child_path = f"{path}.{key}" if path else key
                self._walk_document(child_value, child_path, seen, type_counts, null_counts, examples)
        elif isinstance(value, list):
            child_path = f"{path}[]"
            for item in value:
                self._walk_document(item, child_path, seen, type_counts, null_counts, examples)
        elif path != "" and value is not None and len(examples[path]) < 3:
            examples[path].append(self._safe_example(value))

    @staticmethod
    def _normalize_type(value) -> str:
        """Map a Python/BSON value onto a stable, JSON-friendly type name."""
        if value is None:
            return "null"
        # bool is a subclass of int, so it must be checked first.
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "double"
        if isinstance(value, str):
            return "string"
        if isinstance(value, datetime):
            return "date"
        if isinstance(value, ObjectId):
            return "objectId"
        if isinstance(value, uuid.UUID):
            return "uuid"
        # bson.Binary is a subclass of bytes; UUID subtype-4 binaries also surface here
        # under legacy uuid representations.
        if isinstance(value, (bytes, bytearray)):
            return "binary"
        if isinstance(value, (Decimal, Decimal128)):
            return "decimal"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        return type(value).__name__

    @staticmethod
    def _safe_example(value):
        """Keep JSON-native scalars as-is; stringify anything else (ObjectId, Decimal128...)."""
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def suggest_quality_checks(
        self,
        catalog: Dict[str, Any],
        present_threshold: float = 80.0,
    ) -> List[Dict[str, Any]]:
        """
        Turn an inferred field catalog into a list of *suggested* declarative quality
        checks — the "profile -> suggest -> confirm" pattern the leading platforms use,
        instead of making the user hand-write rules from scratch.

        Two high-signal check types fall straight out of schema-less data and have no
        relational equivalent:

          - field_presence : a field present in most-but-not-all docs is a likely
                             "should always be there" field whose absences are defects.
          - type_consistency : a field that is overwhelmingly one type but occasionally
                             another (e.g. amount: number 98% / string 2%) is drifting.

        Returns suggestions only — nothing is persisted or executed here.
        """
        suggestions = []
        for field in catalog.get("fields", []):
            path = field["path"]
            presence = field["presence_pct"]
            types = field["types"]

            # Mostly-present-but-not-always => candidate required field.
            if present_threshold <= presence < 100.0:
                suggestions.append({
                    "check": "field_presence",
                    "field": path,
                    "params": {"min_presence_pct": round(presence, 0)},
                    "severity": "WARNING",
                    "rationale": (
                        f"'{path}' is present in {presence}% of sampled documents — "
                        f"the missing {round(100 - presence, 1)}% may be defects."
                    ),
                })

            # Mixed types for one field (ignoring null) => candidate type drift.
            non_null = [t for t in types if t["type"] != "null"]
            if len(non_null) > 1:
                dominant = non_null[0]
                minority = ", ".join(f"{t['type']} {t['pct']}%" for t in non_null[1:])
                suggestions.append({
                    "check": "type_consistency",
                    "field": path,
                    "params": {"expected_type": dominant["type"]},
                    "severity": "URGENT",
                    "rationale": (
                        f"'{path}' is dominantly {dominant['type']} ({dominant['pct']}%) "
                        f"but also appears as {minority}."
                    ),
                })
        return suggestions

    def count_table_rows(self, table_name: str) -> int:
        client = self.get_connection()
        try:
            db = client[self.database_name]
            return db[table_name].count_documents({})
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            return 0
        finally:
            client.close()

    def get_min_max_date(self, table_name: str, column_name: str) -> Tuple[Any, Any]:
        """Uses aggregation to find the min and max dates/values for a specific field."""
        client = self.get_connection()
        try:
            db = client[self.database_name]
            pipeline = [
                {"$match": {column_name: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": None,
                    "min_val": {"$min": f"${column_name}"},
                    "max_val": {"$max": f"${column_name}"}
                }}
            ]
            result = list(db[table_name].aggregate(pipeline))
            if result:
                return result[0].get("min_val"), result[0].get("max_val")
            return None, None
        finally:
            client.close()

    # Two-column shape used only as a last-resort fallback when a collection is empty
    # or schema inference fails, so the sync pipeline still has a valid table to target.
    _FALLBACK_TABLE_SCHEMA = [
        {"position": 0, "name": "_id", "type": "TEXT", "length": None, "nullable": "NO",
         "default": None, "primary_key": "YES", "foreign_key": "NO", "is_index": "NO"},
        {"position": 1, "name": "data", "type": "JSONB", "length": None, "nullable": "YES",
         "default": None, "primary_key": "NO", "foreign_key": "NO", "is_index": "NO"},
    ]

    def extract_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Relational landing schema for a collection, derived from its *actual* documents.

        Instead of collapsing every document into ``(id, data)``, this promotes each
        TOP-LEVEL field to its own typed column and keeps nested objects/arrays as JSONB:

          - top-level scalar (string/number/bool/date/_id) -> a typed column, stored as-is
          - top-level object or array                      -> a single JSONB column
          - any top-level field NOT seen while sampling     -> folded into ``_extra`` JSONB

        A field that appears with more than one scalar type across documents is widened to
        TEXT so heterogeneous documents never break the insert. The result is memoized so
        that stream_batch()/fetch_batch() project rows in the exact same column order.

        Falls back to the legacy ``(id, data)`` shape if the collection is empty or
        sampling fails.
        """
        return self._relational_schema(table_name)

    def _relational_schema(self, table_name: str, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """Build (and cache) the promoted-column schema for a collection."""
        if table_name in self._relational_schema_cache:
            return self._relational_schema_cache[table_name]

        try:
            catalog = self.profile_collection_schema(table_name, sample_size=sample_size)
            schema = self._relational_schema_from_catalog(catalog)
        except Exception as exc:
            logger.error(f"Failed to infer relational schema for {table_name}, using fallback: {exc}")
            schema = list(self._FALLBACK_TABLE_SCHEMA)

        self._relational_schema_cache[table_name] = schema
        return schema

    @classmethod
    def _relational_schema_from_catalog(cls, catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Pure transform: field catalog -> ordered list of relational column definitions.

        Kept IO-free so it can be unit-tested. ``_id`` is always the first column and the
        primary key; ``_extra`` is always the last column (drift overflow). Column order is
        deterministic (``_id``, then top-level fields sorted by name, then ``_extra``) so
        the DDL and the streamed rows are guaranteed to line up.
        """
        # Only top-level fields become columns (skip nested "a.b" and array "a[]" paths).
        top_level = {
            f["path"]: f for f in catalog.get("fields", [])
            if "." not in f["path"] and "[]" not in f["path"]
        }
        if not top_level:
            return list(cls._FALLBACK_TABLE_SCHEMA)

        columns: List[Dict[str, Any]] = []

        # _id first, as primary key (drop it from the field map; it is handled explicitly).
        top_level.pop("_id", None)
        columns.append({
            "position": 0, "name": "_id", "type": "TEXT", "length": None,
            "nullable": "NO", "default": None, "primary_key": "YES",
            "foreign_key": "NO", "is_index": "NO",
        })

        for name in sorted(top_level):
            field = top_level[name]
            columns.append({
                "position": len(columns),
                "name": name,
                "type": cls._postgres_type_for(field),
                "length": None,
                # Always nullable: a sample can't prove a field is present in EVERY document
                # of the collection, so NOT NULL would risk failing inserts on later docs.
                # (Presence/required-ness is still surfaced for quality rules via the catalog.)
                "nullable": "YES",
                "default": None,
                "primary_key": "NO",
                "foreign_key": "NO",
                "is_index": "NO",
            })

        # Overflow column for any top-level field not captured by the sample.
        columns.append({
            "position": len(columns), "name": EXTRA_COLUMN, "type": "JSONB", "length": None,
            "nullable": "YES", "default": None, "primary_key": "NO",
            "foreign_key": "NO", "is_index": "NO",
        })
        return columns

    @staticmethod
    def _postgres_type_for(field: Dict[str, Any]) -> str:
        """Pick the destination column type for an inferred top-level field."""
        non_null_types = [t["type"] for t in field.get("types", []) if t["type"] != "null"]
        # Nested object/array anywhere => JSONB.
        if "object" in non_null_types or "array" in non_null_types:
            return "JSONB"
        # Mixed scalar types across documents => widen to TEXT so inserts never fail.
        if len(set(non_null_types)) > 1:
            return "TEXT"
        return POSTGRES_TYPE_MAP.get(field["dominant_type"], "TEXT")

    def _project_document(self, doc: Dict[str, Any], schema: List[Dict[str, Any]]) -> tuple:
        """
        Turn one document into a row tuple matching ``schema`` column order.

        Scalars are passed through natively (psycopg2 adapts them to the typed column);
        objects/arrays and anything destined for a JSONB column are serialized to JSON;
        unmodeled top-level fields are gathered into ``_extra``.
        """
        modeled = {c["name"] for c in schema}
        row = []
        for col in schema:
            name = col["name"]
            if name == "_id":
                row.append(str(doc.get("_id", "")))
            elif name == EXTRA_COLUMN:
                extra = {k: v for k, v in doc.items() if k not in modeled}
                row.append(json.dumps(extra, default=str) if extra else None)
            else:
                value = doc.get(name, None)
                col_type = col["type"]
                if value is None:
                    row.append(None)
                elif col_type == "JSONB" or isinstance(value, (dict, list)):
                    # JSONB column, or a nested value landing in a TEXT column after drift.
                    row.append(json.dumps(value, default=str))
                elif col_type == "BYTEA" and isinstance(value, (bytes, bytearray)):
                    # psycopg2 adapts bytes -> bytea natively; keep it lossless.
                    row.append(bytes(value))
                elif col_type == "UUID":
                    # Pass as text so Postgres casts '...'::uuid; avoids needing
                    # psycopg2.extras.register_uuid() in the sync layer.
                    row.append(str(value))
                else:
                    row.append(self._coerce_scalar(value))
        return tuple(row)

    @staticmethod
    def _coerce_scalar(value):
        """
        Make a scalar safe for a typed Postgres column.

        psycopg2 only adapts native Python types (str/int/float/bool/datetime/Decimal),
        not BSON types. Decimal128 is converted to Decimal so it fits a NUMERIC column;
        any other exotic BSON value (ObjectId, Binary, Timestamp, Regex, UUID, ...) is
        stringified so it lands cleanly in its TEXT column instead of crashing the insert.
        """
        if value is None or isinstance(value, (str, int, float, bool, datetime, Decimal)):
            return value
        if isinstance(value, Decimal128):
            return value.to_decimal()
        return str(value)

        return [
            {
                "position": 0, 
                "name": "id",
                "type": "VARCHAR(255)",
                "length": 255, 
                "nullable": "NO", 
                "default": None, 
                "primary_key": "YES", 
                "foreign_key": "NO",
                "is_index": "NO",
            },
            {
                "position": 1, 
                "name": "data", 
                "type": "JSONB",
                "length": None, 
                "nullable": "YES", 
                "default": None, 
                "primary_key": "NO", 
                "foreign_key": "NO", 
                "is_index": "NO",
            },
        ]

    def create_schema_if_missing(self, schema_name: str):
        """
        In MongoDB, databases and collections are created lazily upon the first insert.
        This function acts as a pass-through or a simple verification.
        """
        logger.info("MongoDB creates databases and collections implicitly upon first insert. Skipping manual creation.")
        pass

    def fetch_deltas(self, primary_key: str, log_table: str, since_ts: datetime, batch_size: int = 10_000):
        """
        Mimics the ROW_NUMBER() window function from SQL by using an Aggregation Pipeline.
        Groups by primary_key, sorts by Date_operation descending, and takes the first document.
        """
        client = self.get_connection()
        try:
            db = client[self.database_name]
            
            pipeline = [
                # 1. Filter records newer than since_ts
                {"$match": {"Date_operation": {"$gt": since_ts}}},
                # 2. Sort by date descending so the newest is first
                {"$sort": {"Date_operation": -1}},
                # 3. Group by the primary key, and keep ONLY the first document (the newest one)
                {"$group": {
                    "_id": f"${primary_key}",
                    "latest_doc": {"$first": "$$ROOT"}
                }},
                # 4. Flatten the structure back to normal
                {"$replaceRoot": {"newRoot": "$latest_doc"}},
                # 5. Final sort (optional, depending on your downstream needs)
                {"$sort": {primary_key: 1, "Date_operation": -1}}
            ]
            
            cursor = db[log_table].aggregate(pipeline, batchSize=batch_size)
            
            for doc in cursor:
                yield doc
                
        finally:
            client.close()

    def get_table_indexes(self, table_name: str) -> list:
        """Returns a list of indexes for the given collection."""
        client = self.get_connection()
        try:
            db = client[self.database_name]
            index_info = db[table_name].index_information()
            
            indexes = []
            for name, info in index_info.items():
                # 'key' is typically a list of tuples like [('field_name', 1)]
                columns = [k[0] for k in info.get('key', [])]
                indexes.append({"name": name, "columns": columns})
            
            logger.info(f"indexes ==> {indexes}")
                
            return indexes
        except Exception as e:
            logger.error(f"Error getting indexes for {table_name}: {e}")
            return []
        finally:
            client.close()

    def build_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]]) -> str:
        """In MongoDB, databases and collections are created lazily upon the first insert."""
        
        return None, None
    
    def create_table_if_missing(self, table_name: str, create_table_statement: str, index_table_statement: str = None):
        """In MongoDB, databases and collections are created lazily upon the first insert."""
        return True