"""PostgreSQL catalog and session primitives for dataset index lifecycle.

No application metadata or authorization policy lives in this module.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import sha256

from psycopg2.sql import SQL, Identifier


class IndexLockBusy(RuntimeError):
    """Retryable; no unbounded advisory-lock wait."""


@dataclass(frozen=True)
class PhysicalIndex:
    oid: int
    schema: str
    table: str
    index_name: str
    definition: str
    valid: bool
    ready: bool
    primary: bool
    constraint_owned: bool
    relation_kind: str
    comment: str | None


def inspect_index(cursor, schema, name):
    cursor.execute('''SELECT i.oid, n.nspname, t.relname, i.relname,
        pg_get_indexdef(x.indexrelid), x.indisvalid, x.indisready, x.indisprimary,
        EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid=i.oid),
        t.relkind, obj_description(i.oid, 'pg_class')
        FROM pg_class i JOIN pg_namespace n ON n.oid=i.relnamespace
        LEFT JOIN pg_index x ON x.indexrelid=i.oid
        LEFT JOIN pg_class t ON t.oid=x.indrelid
        WHERE n.nspname=%s AND i.relname=%s''', (schema, name))
    row = cursor.fetchone()
    return PhysicalIndex(*row) if row else None


def list_indexes(cursor, schema, table):
    cursor.execute('''SELECT i.relname FROM pg_index x
        JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class t ON t.oid=x.indrelid
        JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname=%s AND t.relname=%s ORDER BY i.relname''', (schema, table))
    return [inspect_index(cursor, schema, name) for name, in cursor.fetchall()]


def lock_id(key):
    return int.from_bytes(sha256(key.encode('utf8')).digest()[:8], 'big', signed=True)


@contextmanager
def advisory_lock(connection, key):
    with connection.cursor() as cur:
        cur.execute('SELECT pg_try_advisory_lock(%s)', (lock_id(key),))
        if not cur.fetchone()[0]:
            raise IndexLockBusy('Dataset/index is busy; retry reconciliation')
    try:
        yield connection
    finally:
        if not connection.closed:
            with connection.cursor() as cur:
                cur.execute('SELECT pg_advisory_unlock(%s)', (lock_id(key),))


_sessions = ContextVar('ddp_index_sessions', default=None)


@contextmanager
def dataset_session(factory, collection, dataset_id, schema, table):
    key = f'ddp-index:v1:dataset:{collection}:{dataset_id}'
    held = _sessions.get() or {}
    if key in held:
        yield held[key]
        return
    connection = factory()
    if connection is None:
        raise ConnectionError('Unable to connect to the datamart')
    try:
        connection.autocommit = True
        with advisory_lock(connection, key):
            # Also serialize duplicate metadata identities; callers reject them.
            with advisory_lock(connection, f'ddp-index:v1:relation:{schema}:{table}'):
                token = _sessions.set(dict(held, **{key: connection}))
                try:
                    yield connection
                finally:
                    _sessions.reset(token)
    finally:
        connection.close()


def stamp(cursor, physical, receipt):
    """Reserve a single line; keep all existing organization comment content."""
    prefix = '[ddp-index:v1:'
    lines = (physical.comment or '').split('\n')
    original = '\n'.join(line for line in lines if not line.startswith(prefix))
    comment = original + ('\n' if original else '') + prefix + receipt + ']'
    cursor.execute(SQL('COMMENT ON INDEX {}.{} IS %s').format(
        Identifier(physical.schema), Identifier(physical.index_name)), (comment,))


def drop(cursor, physical):
    if physical.primary or physical.constraint_owned or physical.table is None:
        raise ValueError('Cannot drop a constraint or non-index relation')
    cursor.execute(SQL('DROP INDEX {}{}.{}').format(
        SQL('' if physical.relation_kind == 'p' else 'CONCURRENTLY '),
        Identifier(physical.schema), Identifier(physical.index_name)))
