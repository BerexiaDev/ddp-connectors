# ddp-connectors 0.2.3

Date: 2026-11-02. Release branch: `main`, based on merge commit
`0fd68826fdea1d5bab20c002bf62f040a0b6402e` (PRs #13 and #16).

This release includes Oracle timestamp precision mapping, owner-scoped schema
discovery without duplicate columns, locale-consistent Informix text binding,
the Informix connection-test query, and readable ping errors with retry/resource
cleanup. [Merge decisions and regression coverage](PR-MERGE-20261102.md) explain
how the overlapping changes were reconciled.

The release preparation changes metadata and documentation only; connector
implementation is identical to the reviewed merged tree. The `0.2.2` tag remains
on `bd212bbc8d2eb9226f3aaf260539bcbf37bc3f6b` and is not moved or overwritten.

## Verification

The complete source suite passed 39 tests, with 10 Oracle/Informix integration
tests explicitly skipped because they create fixtures. PostgreSQL integration
ran against a newly created disposable PostgreSQL 16 instance. No existing
database data or DDP core files were changed.

SELECT-only real-driver probes passed:

- Oracle ping, missing-schema feedback and clearing errors after recovery.
- Oracle retained catalog discovery maps `TIMESTAMP(6)` to `TIMESTAMP`.
- Informix ping and parameterized filtering match literal-query results.
- Informix returns `entity`, `caf\u00e9`, and `O'Brien` exactly through binding.

No retained multi-index Oracle fixture was available; its duplicate-column and
cross-owner cases are exercised by a relational in-memory catalog regression.
The earlier UTF-8 and fractional-timestamp live-driver evidence remains in the
[original verification record](CONNECTOR-FIXES-20261102.md). Those fixture-writing
checks were not rerun against existing databases for this release.

Commands used in disposable runtimes, with `DDP_TEST_POSTGRES_DSN` pointing only
at the disposable PostgreSQL instance:

```sh
python -m unittest discover -v -s /candidate/tests
python scripts/smoke_install.py --python /usr/local/bin/python
python scripts/smoke_install.py --python /usr/local/bin/python --constraints /tmp/release-source/tests/consumer-constraints.txt
```

The default smoke command applies to Python 3.10, 3.11 and 3.12. The constrained
profile applies to Python 3.10. Each smoke command builds a wheel from the sdist,
runs strict Twine checks, installs both artifacts into separate clean virtual
environments, checks dependencies and installed version/origin, then runs the
complete suite. Local logs, artifacts and SHA-256 hashes are retained under
the ignored `dist/release-0.2.3/` directory.

| Clean install profile | Wheel | Sdist |
| --- | --- | --- |
| Python 3.10, default dependencies | 39 passed, 10 skipped | 39 passed, 10 skipped |
| Python 3.10, consumer constraints | 39 passed, 10 skipped | 39 passed, 10 skipped |
| Python 3.11, default dependencies | 39 passed, 10 skipped | 39 passed, 10 skipped |
| Python 3.12, default dependencies | 39 passed, 10 skipped | 39 passed, 10 skipped |

All eight installs passed `pip check` and the installed version/origin assertions.
Wheel and sdist passed strict Twine checks for every profile. GitHub's Python
matrix is checked on the release commit before pushing the new release tag.

## Git consumption

The annotated Git tag `0.2.3` must resolve to the release commit on `origin/main`.
Verify with `git ls-remote origin refs/heads/main refs/tags/0.2.3 'refs/tags/0.2.3^{}'`.
Core can adopt the release in a separate change using:

```text
ddp-connectors @ git+https://github.com/BerexiaDev/ddp-connectors.git@0.2.3
```

Rebuild consuming images and rerun the affected E2E scenarios. Existing Oracle-
backed datamart columns created as TEXT still need a separately reviewed migration
or rebuild; a connector upgrade does not change them. No package-index upload or
core dependency update is part of this Git release.
