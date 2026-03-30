from ddp_connectors.database_connectors.document import Document
from ddp_lib.cryptography_utils import encrypt_secret, decrypt_secret


def _make_sensitive_property(field):
    """Factory to create a property that encrypts on set and decrypts on get."""
    private = f"_{field}"

    def getter(self):
        val = self.__dict__.get(private)
        return decrypt_secret(val) if val else None

    def setter(self, value):
        self.__dict__[private] = encrypt_secret(value) if value else None

    return property(getter, setter)


class Connector(Document):
    __TABLE__ = "connectors"

    # Sensitive fields: stored encrypted in MongoDB, decrypted transparently via properties
    SENSITIVE_FIELDS = ('password', 'secret', 'aws_access_key_secret', 'sas_token', 'shared_access_key')

    name=None
    description=None
    aws_access_key_id = None
    aws_s3_region_name = None
    bucket_name = None
    host=None
    port=None
    database=None
    user=None
    url=None
    auth_with=None
    created_on=None
    modified_on=None
    type=None
    conn_string=None
    mode=None
    database_type=None
    driver_path=None

    # Encrypted properties (getter decrypts, setter encrypts)
    password = _make_sensitive_property('password')
    secret = _make_sensitive_property('secret')
    aws_access_key_secret = _make_sensitive_property('aws_access_key_secret')
    sas_token = _make_sensitive_property('sas_token')
    shared_access_key = _make_sensitive_property('shared_access_key')

    def from_dict(self, data):
        """Load from DB: store encrypted values directly into backing attrs, bypassing setters."""
        if data:
            for key, value in data.items():
                if key in self.SENSITIVE_FIELDS:
                    # Store encrypted value directly — bypass setter to avoid double encryption
                    self.__dict__[f"_{key}"] = value
                else:
                    setattr(self, key, value)
        else:
            self._id = None
        return self

    def to_dict(self):
        data = {}
        # Start with class-level defaults so no fields are silently dropped
        for k in vars(self.__class__):
            if not k.startswith("_") and k not in self.SENSITIVE_FIELDS and k != "SENSITIVE_FIELDS":
                val = getattr(self, k, None)
                if not callable(val):
                    data[k] = val
        # Override with instance values
        for k, v in self.__dict__.items():
            if k.startswith("_") and k[1:] in self.SENSITIVE_FIELDS:
                data[k[1:]] = v
            elif not k.startswith("_"):
                data[k] = v
        data["_id"] = getattr(self, "_id", None)
        return data
