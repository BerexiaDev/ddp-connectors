"""Credential-safe database diagnostics. Never attach exceptions or frame locals."""
import ast
import json
import re
import traceback
from urllib.parse import quote, quote_plus

from loguru import logger


SECRET_FIELDS = ('password', 'pwd', 'user', 'username', 'secret', 'token',
                 'conn_string', 'url', 'sas_token', 'shared_access_key',
                 'aws_access_key_id', 'aws_access_key_secret', 'access_token', 'refresh_token')
SECRET_ASSIGNMENT = re.compile(
    r'''(?ix)(\b(?:password|pwd|user(?:name)?|uid|secret|token|access_token|refresh_token|'''
    r'''sas_token|shared_access_key|aws_access_key_secret)["']?\s*[:=]\s*)'''
    r'''(?:\{(?:[^}]|}})*\}|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s;,]+)'''
)


def _redactor(connector):
    secrets = set()
    for key in SECRET_FIELDS:
        value = getattr(connector, key, None)
        if isinstance(value, str) and value:
            variants = {value, value.replace('}', '}}'),
                        quote(value, safe='', errors='surrogatepass'),
                        quote_plus(value, errors='surrogatepass')}
            # Drivers may quote credentials before DB-API quotes the whole message.
            for _ in range(2):
                variants.update(escaped for item in tuple(variants)
                                for escaped in (repr(item)[1:-1], json.dumps(item)[1:-1]))
            secrets.update(variants)
    secrets = sorted(secrets, key=len, reverse=True)

    def redact(value):
        if isinstance(value, dict):
            return {key: redact(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [redact(item) for item in value]
        if not isinstance(value, str):
            return value if value is None or isinstance(value, (int, float, bool)) else '<omitted>'
        # Decode DB-API tuple repr before matching quoted credential assignments.
        if re.match(r'''^\(['"]''', value):
            try:
                if len(value) > 60000:
                    raise ValueError('Oversized diagnostic')
                parts = ast.literal_eval(value)
                if isinstance(parts, tuple):
                    return str(tuple(redact(part) if part is None or isinstance(part, (str, int, float, bool))
                                     else '<omitted>' for part in parts))[:6000]
            except (ValueError, SyntaxError, MemoryError, RecursionError):
                return '<unparseable driver diagnostic omitted>'
        # Match the original text: masking a short username must not hide PWD keys
        # or URI schemes, and generic masking must not split a known password.
        spans = [(m.start(), m.end()) for secret in secrets
                 for m in re.finditer(re.escape(secret), value)]
        for pattern in (SECRET_ASSIGNMENT,
                        r'(?i)([a-z][a-z0-9+.-]*://)[^/\s@]+@',
                        r'(?i)(\b(?:Bearer|Basic)\s+)\S+'):
            spans.extend((m.end(1), m.end()) for m in re.finditer(pattern, value))
        merged = []
        for start, end in sorted(spans):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        for start, end in reversed(merged):
            value = value[:start] + '[REDACTED]' + value[end:]
        return value[:6000]

    return redact


def log_failure(connector, operation, error, **context):
    """Emit one JSON event and return safe details, without changing driver errors."""
    try:
        redact = _redactor(connector)
        message = str(error)
        target = {key: getattr(connector, key, None) for key in
                  ('host', 'port', 'database', 'driver', 'driver_path', 'protocol', 'locale')}
        target['database'] = target['database'] or getattr(connector, 'database_name', None)
        target['type'] = type(connector).__name__
        details = dict(operation=operation, connector=target, error=message,
                       exception_type=type(error).__name__)
        if context:
            details['context'] = context
        # Only frame locations: no source lines, locals, exception chains or SQL params.
        if isinstance(error, BaseException):
            details['traceback'] = '\n'.join(
                f'{frame.filename}:{frame.lineno} in {frame.name}'
                for frame in traceback.extract_tb(error.__traceback__)[-20:])
        args = getattr(error, 'args', ())
        state = getattr(error, 'sqlstate', None) or getattr(error, 'pgcode', None)
        if not state and args and isinstance(args[0], str) and re.fullmatch(r'[A-Z0-9]{4,5}', args[0]):
            state = args[0]
        if state:
            details['reported_sqlstate'] = str(state)
        match = re.search(r'\((SQL[A-Za-z0-9]+)\)', message)
        if match:
            details['odbc_operation'] = match[1]
        details = redact(details)
    except Exception:
        # Broken driver formatting or redaction must fail closed and preserve control flow.
        details = {'error': 'Diagnostic preparation failed; driver details omitted.'}
    logger.opt(exception=None).error('connector_error {}', json.dumps(details, ensure_ascii=True))
    return details


def record_ping_failure(connector, error):
    """Keep the boolean ping API and its friendly message, with separate diagnostics."""
    try:
        describe = getattr(connector, 'describe_error', None)
        message = describe(error) if callable(describe) else str(error)
        connector.last_error = _redactor(connector)(message)
    except Exception:
        connector.last_error = 'Database connection failed; driver details omitted.'
    connector.last_error_details = log_failure(connector, 'ping', error)
