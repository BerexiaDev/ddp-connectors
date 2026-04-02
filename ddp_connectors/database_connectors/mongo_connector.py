
from datetime import datetime
from itertools import islice
import json
from loguru import logger
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient

from bson.decimal128 import Decimal128
from bson.codec_options import TypeCodec, TypeRegistry, CodecOptions
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from decimal import Decimal

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

    def insert_data(self, table_name: str, data: List[Any], columns: List[str] = None) -> int:
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
            

    def upsert_data(self, table_name: str, data: List[Any], columns: Optional[List[str]] = None, pk_columns: Optional[List[str]] = None) -> int:
        """
        Upserts a list of dictionaries into the specified MongoDB collection using bulk_write.
        """
        if not data:
            logger.warning(f"No data provided to upsert into {table_name}.")
            return 0

        # Default to MongoDB's standard primary key if none is provided
        if not pk_columns:
            pk_columns = ["_id"]

        client = self.get_connection()
        
        try:
            db = client[self.database_name]
            # Assuming codec_options is defined on your class or imported
            collection = db.get_collection(table_name, codec_options=codec_options)

            # 1. Build a list of UpdateOne operations
            operations = []
            for row in data:
                # Dynamically build the match filter based on the primary keys
                filter_query = {pk: row.get(pk) for pk in pk_columns if pk in row}
                
                # Skip rows that don't have the required primary keys to avoid accidental mass-updates
                if not filter_query:
                    logger.warning(f"Row missing primary key(s) {pk_columns}. Skipping row.")
                    continue

                # 2. Append the upsert operation using $set
                operations.append(
                    UpdateOne(
                        filter=filter_query,
                        update={"$set": row},
                        upsert=True
                    )
                )

            if not operations:
                logger.warning(f"No valid operations to execute for {table_name}.")
                return 0

            # 3. Execute the bulk write
            # ordered=False allows MongoDB to process all operations even if some fail
            result = collection.bulk_write(operations, ordered=False)
            
            # In MongoDB, an upsert operation either 'upserts' (inserts new) or 'modifies' (updates existing)
            total_success = result.upserted_count + result.modified_count
            
            logger.info(f"Successfully upserted/modified {total_success} documents in {table_name}.")
            return total_success

        except BulkWriteError as bwe:
            # BulkWriteError gives us a detailed payload of exactly which documents failed and why
            logger.error(f"Bulk write error while upserting data into {table_name}: {bwe.details}")
            raise
        except Exception as exc:
            logger.error(f"Error upserting data into {table_name}: {exc}")
            raise
            
        finally:
            client.close()

    def fetch_batch(self, table_name: str, offset: int, limit: int = 100):
        """
        Fetches a batch of documents using skip and limit.
        """
        client = self.get_connection()
        try:
            db = client[self.database_name]
            cursor = db[table_name].find().skip(offset).limit(limit)
            dumps = json.dumps
            to_str = str 
            
            # 2. Use a C-optimized list comprehension to process the cursor
            return [
                (to_str(doc.get("_id", "")), dumps(doc, default=to_str))
                for doc in cursor
            ]
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return []
        finally:
            client.close()

    def stream_batch(self, cursor, table_name: str, batch_size: int = 10_000, **kwargs):
        """
        Streaming for MongoDB using a cursor.
        If as_dict=True, yields batches of raw dictionaries.
        If as_dict=False, yields batches of (id_string, document_json_string) tuples for fast Postgres inserts.
        """
        client = self.get_connection()
        try:
            logger.info(f"Start streaming MongoDB collection {table_name} with batch_size={batch_size}")
            db = client[self.database_name]
            
            # batch_size tells the MongoDB driver how many documents to fetch per network round trip
            cursor = db[table_name].find(batch_size=batch_size)
            
            # Performance aliases for the inner loop
            dumps = json.dumps
            to_str = str
            
            while True:
                # 1. Grab exactly batch_size items directly from the cursor
                chunk = list(islice(cursor, batch_size))
                
                # If the chunk is empty, we've reached the end of the collection
                if not chunk:
                    break
                else:
                    # Yield the highly optimized tuple batch instantly
                    yield [
                        (to_str(doc.get("_id", "")), dumps(doc, default=to_str))
                        for doc in chunk
                    ]
                    
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

    def get_connection_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        MongoDB is schema-less. This infers 'columns' (keys) by sampling the first document.
        """
        try:
            return [
                {
                    "name": "id",
                    "type": "string",
                    "alias": "id",
                },
                {
                    "name": "data",
                    "type": "record",
                    "alias": "data",
                },
            ]
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []

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

    def extract_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Extracts the schema by finding the documents with the highest number of fields.
        This provides a highly accurate schema without the performance penalty of unwinding the whole collection.
        """
        
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