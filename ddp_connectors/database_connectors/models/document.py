from pymongo import UpdateOne
from app.main import mongo
from ddp_lib.utils.utils import generate_id


class Document:
    __TABLE__ = None
    _id = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    def db(self):
        return mongo.db[self.__TABLE__]

    def save(self):
        """Save or update the document in MongoDB."""
        if not self._id:
            self._id = generate_id()
        self._id = self.db().save(self.to_dict())
        return self

    def update(self, query=None, set_fields=None, **kwargs):
        if not query:
            query = {"_id": self._id}
        if set_fields is None:
            set_fields = self.to_dict()
        self.db().update_one(query, {"$set": set_fields}, **kwargs)

    @classmethod
    def update_many(cls, query: dict, set_fields: dict,  **kwargs,):
        """
        Update all documents matching `query`, setting the keys/values in `set_fields`.
        """
        return cls().db().update_many(query, {"$set": set_fields}, **kwargs)

    def save_all(self, items):
        """Insert multiple new documents into the collection."""
        for item in items:
            if not item.get("_id"):
                item["_id"] = generate_id()
        self.db().insert_many(items)

    def upsert_all(self, items):
        """Perform bulk upsert (insert or update) operations."""
        operations = [UpdateOne({"_id": item.get("_id", generate_id())}, {'$set': item}, upsert=True) for item in items]
        if operations:
            self.db().bulk_write(operations)

    def load(self, query=None):
        """Load a single document from the database."""
        if not query:
            query = {"_id": self._id}
        result = self.db().find_one(query)
        if result:
            self.from_dict(result)
        return self

    def delete(self, query=None):
        """Delete a document from the database."""
        query = query or ({"_id": self._id} if self._id else None)
        if query:
            self.db().delete_one(query)
        return self

    def delete_many(self, query={}):
        """Delete multiple documents from the collection."""
        self.db().delete_many(query)
        return self


    def to_dict(self):
        """Convert this document to a dictionary."""
        return self.__dict__

    def from_dict(self, data):
        """Update this document's attributes from a dictionary."""
        if data:
            for key, value in data.items():
                setattr(self, key, value)
        else:
            self._id = None
        return self

    def aggregate(self, pipeline):
        """Perform an aggregation pipeline query."""
        return list(self.db().aggregate(pipeline))

    @classmethod
    def get_all(cls, query=None, sort=None, skip=0, limit=None, projection=None):
        """
        Retrieve all documents matching a query with optional sorting, pagination, and projection.

        :param query: dict, The filter query (default: {}).
        :param sort: list, Sorting criteria (e.g., [("_id", ASCENDING)]).
        :param skip: int, Number of documents to skip (for pagination).
        :param limit: int, Maximum number of documents to return.
        :param projection: dict, Fields to include/exclude (e.g., {"name": 1, "email": 1}).
        :return: list of cls instances representing documents.
        """
        try:
            query = query or {}  # Ensure query is a dictionary

            cursor = cls().db().find(query, projection)

            if sort:
                cursor = cursor.sort(sort)

            if skip > 0:
                cursor = cursor.skip(skip)

            if limit is not None and limit > 0:
                cursor = cursor.limit(limit)

            return [cls(**doc) for doc in cursor]

        except Exception as e:
            return []

    @classmethod
    def count(cls, query=None):
        """
        Count documents matching the query.
        
        :param query: dict, The filter query (default: {}).
        :return: int, Number of matching documents.
        """
        query = query or {}
        return cls().db().count_documents(query)

    @classmethod
    def drop(cls):
        """Drop the collection associated with this class."""
        return cls().db().drop()

    @classmethod
    def get_attributes(cls):
        """Get the attributes of this class that are not callable and do not begin with '__'."""
        return [attr for attr in cls.__dict__ if not callable(getattr(cls, attr)) and not attr.startswith("__")]
