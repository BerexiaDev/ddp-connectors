# ddp-connectors 0.2.3.1

Date: 2026-11-03. Release branch: `main`, based on
`66944d27602da1447d8b073bf26585a63577dfae` (release 0.2.3).

## Changes

Database failures now emit credential-redacted `connector_error` JSON events.
They preserve driver messages, reported SQLSTATEs, ODBC operations, target
context and stack frame locations without attaching exception locals. Informix
diagnostics retain the numeric driver message even when `last_error` supplies
readable guidance. Failed database pings also expose `last_error_details`;
both error fields reset before the next ping.

Existing database exception-log sites use the shared sanitizer. PostgreSQL
session debug snapshots retain counts without row samples or session usernames.
Constructors, boolean ping results, retries, queries, transaction handling and
database data remain unchanged. Re-raised driver exceptions still contain their
original text; applications must sanitize their own logs and responses.

## Verification

Release checks use Python 3.10 and isolated installations:

```sh
python -m unittest discover -s tests
python scripts/smoke_install.py
python scripts/smoke_install.py --constraints tests/consumer-constraints.txt
python -m build --outdir dist/release-0.2.3.1/artifacts
python -m twine check --strict dist/release-0.2.3.1/artifacts/*
```

Each smoke command installs wheel and sdist separately, checks dependency
consistency and installed version/origin, and runs the complete test suite.
Release logs, retained artifacts and SHA-256 hashes are kept in the ignored
`dist/release-0.2.3.1/` directory. CI additionally exercises Python 3.10, 3.11
and 3.12 on Ubuntu with a disposable PostgreSQL service.

No client VM or live Informix connection is used in these release checks. The
original Informix connection failure remains undiagnosed until runtime evidence
is available. Database fixture tests require separately configured disposable
servers; this release does not migrate or initialize existing databases.

## Git consumption

The annotated `0.2.3.1` tag must resolve to the release commit on `origin/main`.
Existing tags, including `0.2.3`, remain unchanged. Consumers can adopt:

```text
ddp_connectors@git+https://github.com/BerexiaDev/ddp-connectors.git@0.2.3.1
```

API and Sync pins must both be updated in a separate consumer change before
rebuilding their images. The API diagnostics patch accepts structured details
from this release and retains its compatibility path for 0.2.3. No package-index
upload, consumer pin update or deployment is part of this Git release.
