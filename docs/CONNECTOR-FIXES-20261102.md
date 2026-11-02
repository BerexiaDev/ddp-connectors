# Oracle timestamp and Informix text binding verification

Date: 2026-11-02. Starting checkout: `7486dc9` (`Release 0.2.1`), branch
`feat/kan-5-consolidation`. The pre-existing `.gitignore` edit and `.vscode/`
were preserved. No applicable `AGENTS.md` was found. No core files, dependency
pins, existing datamart columns, or running application packages were changed.

## Changes and contracts

Oracle catalog types such as `TIMESTAMP(6)` missed an exact dictionary lookup
and became `TEXT`. The mapper now recognizes only timestamp declarations with
optional precision 0–9 and either timezone suffix. Plain timestamps remain
`TIMESTAMP`; both timezone variants remain `TIMESTAMPTZ`. Case/whitespace are
accepted. Other mappings, malformed/unknown string fallback (`TEXT`), and
non-string error behavior are unchanged.

`extract_table_schema` already calls this mapper. Its `length` field is Oracle
storage length, not fractional precision. The PostgreSQL DDL builder correctly
uses the mapped base type and does not append that length to timestamps; no
metadata or DDL API changes were necessary.

The source precision constraint is not reproduced. Oracle accepts 0–9 digits;
PostgreSQL and Python datetime have microsecond resolution. The tested Oracle
driver truncates `.123456789` to `.123456` when fetching a Python datetime.
There is no claim of nanosecond preservation. See the
[Oracle type reference](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html)
and [PostgreSQL date/time reference](https://www.postgresql.org/docs/12/datatype-datetime.html).

Informix configured inbound decoding but left outbound text on pyodbc's wide
Unicode default, which this DataDirect driver rejected. `_open_connection`
now selects Latin-1 for locale codesets `819`/`8859-1`, or UTF-8 for
`utf8`/`57372`, and uses SQL_CHAR for outbound text and retrieval of both
character families. The existing single locale still sets both `CLIENT_LOCALE`
and `DB_LOCALE`; it must match the database. No new factory parameters or cursor
wrappers were added. Unsupported/empty values fail with `ValueError` before
connection creation; an omitted factory key still raises `KeyError`.
Unrepresentable/invalid Unicode raises an encoding error instead of substitution.
SQL remains parameterized, and retries/cleanup remain in the existing paths.
Codeset aliases follow the [Informix codeset table](https://help.hcl-software.com/hclinformix/1410/jdbc/ids_jdbc_270.html);
connection-level configuration follows [pyodbc's Unicode contract](https://github.com/mkleehammer/pyodbc/wiki/Unicode).

## Regression evidence

The original diagnostic was run before implementation in a disposable
`ddp-api:latest` container on `deepkube-data-platform_ddp-network`:

- Oracle's retained `UPDATED_AT` catalog type was `TIMESTAMP(6)`; discovery
  returned `TEXT`. Both precision-qualified timezone variants also returned TEXT.
- The retained Informix `WHERE kind = ?`, with `["entity"]`, raised the reported
  DataDirect `1020`/`SQLExecDirectW` error. Default bound string round trips failed.
- A connection-local UTF-8 experiment returned `caf\u00c3\u00a9` for
  `caf\u00e9`; Latin-1 returned `entity`, `caf\u00e9`, and `O'Brien` exactly.

New regressions were run before production edits: 16 test methods,
52 failing subtests/assertions, 4 driver errors, 2 UTF-8 tests skipped while
that fixture was being provisioned. Failures included the actual Oracle
catalog-to-PostgreSQL DDL path and actual Informix string binding. Numeric and
timestamp parameter baselines passed.

Final coverage is 30 unit/contract tests and 11 live tests (41 total):

| Verification | Result |
| --- | --- |
| Source suite without live settings | 30 passed, 11 explicitly skipped |
| Source suite with all live settings | 41 passed, no skips |
| Fresh wheel install, default dependencies | 41 passed, no skips; `pip check` passed |
| Fresh sdist install, default dependencies | 41 passed, no skips; `pip check` passed |
| Fresh wheel install, consumer constraints | 41 passed, no skips; `pip check` passed |
| Fresh sdist install, consumer constraints | 41 passed, no skips; `pip check` passed |
| Wheel/sdist metadata | `twine check --strict` passed for both profiles |
| Read-only retained E2E fixture checks | Oracle TIMESTAMP discovery and Informix bound filter passed |
| Independent diff review | No production blockers; documentation and fixture-schema nits corrected |

Live checks used Linux Python 3.10.22, pyodbc 5.3.0, python-oracledb 3.4.2,
Oracle XE 21, Informix 14.10.FC7W1DE with the existing DataDirect driver, and
the local PostgreSQL 12 datamart. The host's Python 3.9 is below the package
minimum and was not used as a validation runtime.

Oracle tests create uniquely named tables, read real catalog metadata for
precisions 0/3/6/9 and both timezone variants, execute the discovered DDL on
PostgreSQL, and transfer a plain timestamp with `.654321` intact. Fetch and
string extraction paths are covered. Fixture datetime binds explicitly use
`DB_TYPE_TIMESTAMP`: a default python-oracledb DATE bind would discard fractions
during fixture insertion. Zone-aware **values**, DST transitions, original
zone names, and session-local time conversions were not round-trip verified.

Informix tests insert, filter by, and retrieve bound text keys and values in
session-temporary VARCHAR/NVARCHAR tables. Latin-1 preserves ASCII, accents,
and apostrophes; Euro fails explicitly. A newly created disposable UTF-8 database
preserves those values plus Euro and Chinese text. Both tests confirm the actual
database codeset from `sysmaster:sysdbslocale`. Invalid UTF-8 surrogates fail
explicitly. Integer and fractional datetime binds still work. Additional
read-only probes verified every accepted codeset alias, including `en_US.UTF8`.
Cleanup and transient/permanent setup failures are tested with injected errors;
they are not claimed as real network-fault tests.

The DataDirect library emits its existing numeric diagnostics at process exit
even for passing checks; no assertions ignore a binding/retrieval exception.
Installing build tooling in the disposable application image emitted an
unrelated existing pandas-profiling/requests conflict warning. Each clean
candidate virtual environment passed its own `pip check`.

## Commands and reproducibility

Run in Python 3.10+ with installed package dependencies and system drivers:

```sh
python -m unittest discover -v -s tests
python scripts/smoke_install.py
python scripts/smoke_install.py --constraints tests/consumer-constraints.txt
```

Live tests require private environment values, never committed or printed:

- `DDP_TEST_ORACLE_SETTINGS`: JSON of ordinary Oracle factory settings; the
  configured schema needs permission to create/drop the test's own tables.
- `DDP_TEST_INFORMIX_SETTINGS`: JSON for a Latin-1 database.
- `DDP_TEST_INFORMIX_UTF8_SETTINGS`: JSON for a genuinely UTF-8 database.
- `DDP_TEST_INFORMIX_DRIVER`: optional absolute driver path (needed when a clean
  install test changes working directory).
- `DDP_TEST_POSTGRES_DSN`: disposable-test-capable PostgreSQL connection string.

Omitting settings skips the corresponding integration, rather than passing it.
Oracle tests drop only their generated tables. Informix uses temporary tables
closed with the connection. New PostgreSQL DDL is rolled back; the pre-existing
PostgreSQL test creates and drops its own unique schema.

The exact unit command used on the Windows host was:

```powershell
docker run --rm --mount 'type=bind,source=C:\Users\PC\Documents\work\ddp-connectors,target=/candidate,readonly' -e PYTHONPATH=/candidate -e PYTHONDONTWRITEBYTECODE=1 --entrypoint python ddp-api:latest -m unittest discover -v -s /candidate/tests
```

For live/installed checks, the local ignored harness
`docs/superpowers/plans/run_checks.py` loads existing connection metadata privately
from MongoDB and datamart environment values. `$envPath` was a temporary file
populated privately from `docker inspect ddp-api`; `$dbName` named the UTF-8
database created for this task. Neither is a release artifact. Commands used:

```powershell
$mount = 'type=bind,source=C:\Users\PC\Documents\work\ddp-connectors,target=/candidate,readonly'
# Targeted regressions (run red, then green)
docker run --rm --env-file $envPath --network deepkube-data-platform_ddp-network --mount $mount -e PYTHONDONTWRITEBYTECODE=1 -e LIVE_CHECKS=1 -e "INFORMIX_UTF8_DATABASE=$dbName" --entrypoint python ddp-api:latest /candidate/docs/superpowers/plans/run_checks.py test_oracle_types test_informix_encoding test_oracle_live test_informix_live
# Full source suite
docker run --rm --env-file $envPath --network deepkube-data-platform_ddp-network --mount $mount -e PYTHONDONTWRITEBYTECODE=1 -e LIVE_CHECKS=1 -e "INFORMIX_UTF8_DATABASE=$dbName" --entrypoint python ddp-api:latest /candidate/docs/superpowers/plans/run_checks.py
# Clean wheel/sdist installs; builds use a copy under /tmp inside this container
docker run --rm --env-file $envPath --network deepkube-data-platform_ddp-network --mount $mount -e PYTHONDONTWRITEBYTECODE=1 -e LIVE_CHECKS=1 -e RUN_INSTALL_SMOKE=1 -e "INFORMIX_UTF8_DATABASE=$dbName" --entrypoint python ddp-api:latest /candidate/docs/superpowers/plans/run_checks.py
docker run --rm --env-file $envPath --network deepkube-data-platform_ddp-network --mount $mount -e PYTHONDONTWRITEBYTECODE=1 -e LIVE_CHECKS=1 -e RUN_INSTALL_SMOKE=1 -e "INFORMIX_UTF8_DATABASE=$dbName" --entrypoint python ddp-api:latest /candidate/docs/superpowers/plans/run_checks.py --constraints /tmp/connector-source/tests/consumer-constraints.txt
```

The smoke script runs `python -I -m unittest discover -v -s <copied-project>/tests`
in each clean environment and verifies installed origin and absence of ddp-lib.
The temporary UTF-8 database and private environment file were removed after
verification. No existing database/schema/table, user, reference, or volume was
dropped. The application containers continue using their original package.
The first database cleanup attempt used the Latin-1 login locale and was
rejected with locale mismatch 23197. Retrying for the exact task-created name
with `DB_LOCALE=en_us.utf8 CLIENT_LOCALE=en_us.utf8 dbaccess - -` succeeded;
the catalog then confirmed zero remaining entries for that name.

## Core adoption and remaining work

1. The release follow-up updates package metadata and installed-version checks
   to **0.2.2**. Consume the annotated Git tag `0.2.2` after verifying that its
   remote peeled target equals the release commit. The earlier live-driver
   results above were collected before the version bump; they are historical
   evidence for the unchanged connector implementation.
2. In a separate authorized core change, update connector pins in consuming
   services, rebuild their images and redeploy. Do not patch running containers.
3. Rerun Oracle schema discovery, fresh destination creation, initial load and
   delta refresh with fractional timestamps. Check PostgreSQL's actual column
   type. Add timezone value/session/DST cases if those source types are used.
4. Rerun Informix filtered log replay and text-key operations with `entity`,
   `caf\u00e9`, and `O'Brien`; include UTF-8 data on a UTF-8 source and numeric/
   timestamp filters. Keep bound SQL parameters throughout.
5. Inventory Oracle-backed datamart columns already created as TEXT. Core refresh
   retains table structure, so upgrading alone does not repair them. Separately
   review a rebuild or explicit migration after validating existing text values,
   parsing rules, timezone assumptions, and dependent objects. No such migration
   was performed here.

Full core E2E flows were not rerun. Other Informix codesets/drivers are unsupported or unverified; support
must be added with explicit codec and real-driver evidence. Population preview
work remains outside this change. The initial fix implementation made no commit,
push, tag or publication; the separately authorized 0.2.2 follow-up performs
the Git release without changing core or existing database data.

## 0.2.2 release verification

Package metadata, the clean-install version assertion, changelog and release
procedure were updated to 0.2.2. The connector implementation is unchanged from
the real-driver verification above.

The release source suite passed 31 tests (30 unit/contracts plus PostgreSQL
integration), with 10 Oracle/Informix integration tests explicitly skipped.
PostgreSQL 16 ran in a new disposable container on its own network; no existing
database data was changed. SELECT-only probes against retained fixtures also
passed Oracle TIMESTAMP discovery and Informix bound ASCII/accent/quote retrieval.

| Clean install profile | Wheel | Sdist |
| --- | --- | --- |
| Python 3.10, default dependencies | 31 passed, 10 skipped | 31 passed, 10 skipped |
| Python 3.10, consumer constraints | 31 passed, 10 skipped | 31 passed, 10 skipped |
| Python 3.11, default dependencies | 31 passed, 10 skipped | 31 passed, 10 skipped |
| Python 3.12, default dependencies | 31 passed, 10 skipped | 31 passed, 10 skipped |

Every profile passed `pip check`, installed-origin/version assertions, and
`twine check --strict` for wheel and sdist. Commands run from disposable source
copies, with `DDP_TEST_POSTGRES_DSN` pointing only at the new test container:

```sh
python -m unittest discover -v -s /candidate/tests
python scripts/smoke_install.py --python /usr/local/bin/python
python scripts/smoke_install.py --python /usr/local/bin/python --constraints /tmp/release-source/tests/consumer-constraints.txt
```

The constrained command applies to Python 3.10 only. Tests used `ddp-api:latest`
for Python 3.10 and `python:3.11-slim` / `python:3.12-slim` with `libodbc2` for
the other runtimes. Build tooling was installed in a separate virtual environment.
Oracle/Informix tests that create fixtures were not repeated during release;
the prior complete live-driver results remain recorded separately above.
Local release artifacts, hashes and smoke logs are retained under the ignored
`dist/release-0.2.2/` directory. The Git release does not publish to a package index.
