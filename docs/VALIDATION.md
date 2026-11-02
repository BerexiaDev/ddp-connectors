# KAN-5 validation record

Initial consolidation checks ran on Windows x86-64, 2026-10-06. Candidate version:
0.2.0. On 2026-10-07, the package was made independent of ddp-lib at the user's
request. CI now checks out only ddp-connectors and uses no custom credential.
The earlier private-checkout failure is superseded by removal of that checkout.

## Independent package verification (2026-10-07)

- Source suite: 20 passed; optional live PostgreSQL test skipped.
- Existing Oracle serialization baselines passed before the move; importing the
  Oracle factory with ddp-lib blocked failed before the move and passes afterward.
- Python 3.10.21 clean wheel/sdist installs with default dependencies: 20 passed
  in each; optional live PostgreSQL test skipped; pip check and strict twine passed.
- Python 3.11.16 and 3.12.14 clean non-editable installs: 20 passed in each;
  optional live PostgreSQL test skipped; dependency consistency checks passed.
- Every clean install asserts that neither the ddp-lib distribution nor the
  ddp_lib module is present, while importing every connector and checking factories.
- All seven complete consumer requirement profiles resolved again for
  Linux/Python 3.10, preserving every non-library pin.
- Workflow YAML parsed successfully; checks confirm one checkout, contents: read,
  and no custom secret, sibling reference or dependency checkout argument.
- Independent review found no remaining source, contract or packaging blockers.
- Python 3.10.21 clean wheel/sdist installs with existing consumer pins: 20 passed
  in each; optional live PostgreSQL test skipped; pip check and strict twine passed.

These are local results; the revised GitHub-hosted workflow has not yet run.

## Initial consolidation verification (2026-10-06)

| Check | Result |
|---|---|
| Python 3.10.21 source suite | 16 passed; optional live PostgreSQL test run separately |
| Python 3.11.16 clean, non-editable installed package | 16 passed; live PostgreSQL skipped; uv pip check passed |
| Python 3.12.14 clean, non-editable installed package | 16 passed; live PostgreSQL skipped; uv pip check passed |
| Python 3.10 wheel + sdist, default dependencies | Both passed: 16 tests each (live test skipped), pip check and strict twine checks |
| Python 3.10 wheel + sdist, consumer constraint profile | Both passed: 16 tests each (live test skipped), pip check and strict twine checks |
| Complete existing consumer requirements, Python 3.10/Linux resolution | All 7 profiles resolved without changing any non-library pin |
| Independent review | No open blocking source/metadata findings; both identified issues fixed with regressions |
| git diff --check | Passed |

A disposable PostgreSQL 16 container executed the live regression successfully:
quoted schema/table/column/index names, a parent with exactly one composite PK,
December-to-January range partition, default partition, and insertion using copied
literal defaults. The generated schema was dropped and the container removed.
Set DDP_TEST_POSTGRES_DSN to run tests/test_postgres_live.py; it is skipped when
no test database is configured. CI configures an isolated PostgreSQL service.

The common contract suite was also run against pinned connector releases 0.0.12
and 0.0.18 (5 checks each), and the 3 applicable SQL/schema/byte checks against
0.0.5.8. Current canonical factory behavior is tested separately, including SFTP.

## Consumer dependency evidence

Requirements were copied outside consumer repositories, replacing only the two
library references with the local canonical candidates, then resolved with:

    uv pip compile <copied-requirements> --python-version 3.10 --python-platform x86_64-manylinux2014

The unchanged non-library requirements resolved for:
- deepkube-data-platform: API, Sync, Auth and Migration (checkout 018d06d).
- data-platform: API, Sync and Auth (checkout ee08a76).

These retain each consumer's Flask/Werkzeug/Mongo/PyJWT/NumPy/pandas/Oracle pins,
including Werkzeug 2.2.3 and 2.3.8 profiles. The clean-install constraint test
uses the older shared pins; it is not presented as a complete application install.

## Regression evidence and limits

Every ported behavior has a failing-before/passing-after regression. Follow-up
review also reproduced and fixed structured JWT subjects in strict mode and
non-ASCII SQL keyword/numeric lookalikes; quoted Unicode literals remain valid.

No donor or consumer source was modified. Original canonical checkouts remain
unchanged; changes are isolated on feat/kan-5-consolidation worktrees. SQL recovery
and consumer upgrades remain KAN-7/KAN-8; broad token redesign remains KAN-9.
Strict authentication and literal defaults are additive opt-ins.

Import/factory checks cover all shipped modules and Python drivers; they do not
prove connection behavior against every external database, vendor ODBC driver,
JDBC jar or SFTP server. Full service integration belongs to the adoption ticket.
No extra CI credential or sibling checkout is required. No tags, publication or
consumer rollout were performed.
