"""Driver-specific transport errors. SQL, auth and unknown errors fail immediately."""
import errno


def postgres_connection_error(error, *, connecting=False):
    import psycopg2
    if not isinstance(error, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return False
    state = error.pgcode
    if state:
        return state in {'08000', '08001', '08003', '08006', '08007', '08P01', '57P01', '57P02', '57P03'}
    # libpq transport errors often have no server-provided SQLSTATE.
    message = str(error).lower()
    return any(text in message for text in (
        'connection refused', 'connection reset', 'connection already closed',
        'server closed the connection unexpectedly', 'could not receive data from server',
        'could not send data to server', 'ssl syscall error: eof detected',
        'ssl connection has been closed unexpectedly',
    )) or (connecting and 'timeout expired' in message)


def mysql_connection_error(error, *, connecting=False):
    import mysql.connector
    if not isinstance(error, mysql.connector.Error):
        return False
    if error.sqlstate and error.sqlstate.startswith(('22', '23', '28', '40', '42')):
        return False
    # Client transport errors, not server query/lock timeouts or deadlocks.
    return error.errno in {2002, 2003, 2006, 2013, 2055, 4033}


def oracle_connection_error(error, *, connecting=False):
    import oracledb
    if not isinstance(error, oracledb.Error):
        return False
    detail = error.args[0] if error.args else None
    code = getattr(detail, 'code', None)
    full_code = getattr(detail, 'full_code', '')
    if code in {3113, 3114, 3135, 12170, 12535, 12537, 12541, 12543, 12547, 12570, 12571}:
        return True
    if full_code in {'DPY-1001', 'DPY-4011', 'DPI-1010', 'DPI-1080'}:
        return True
    # DPY-6005 wraps several causes, including permanent configuration errors.
    cause = error.__cause__
    return full_code == 'DPY-6005' and isinstance(cause, OSError) and cause.errno in {
        errno.ECONNREFUSED, errno.ECONNRESET, errno.ETIMEDOUT, errno.EHOSTUNREACH,
    }


def jdbc_connection_error(error, *, connecting=False):
    """Read real JDBC SQLSTATEs, including JayDeBeApi's wrapped SQLException."""
    import jaydebeapi
    pending = [error]
    states = set()
    seen = set()
    while pending and len(seen) < 16:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        get_state = getattr(item, 'getSQLState', None)
        if callable(get_state):
            state = get_state()
            if state:
                states.add(str(state))
            get_next = getattr(item, 'getNextException', None)
            if callable(get_next):
                following = get_next()
                if following is not None:
                    pending.append(following)
        if isinstance(item, jaydebeapi.Error):
            pending.extend(arg for arg in item.args if not isinstance(arg, (str, int)))
        cause = getattr(item, '__cause__', None)
        if cause is not None:
            pending.append(cause)
    return bool(states) and states <= {'08000', '08001', '08003', '08006', '08S01'}
