# KAN-5 semantic consolidation audit

## Baselines and coverage

- Donor cmr-connector-lib/main: 66c585539871606db908e798bb0f5ef7ebddd60f.
- Canonical ddp-connectors/main: f191ca12a1cc1f693ee3a0633dc305870057a06c.
- Full non-shallow history: 204 donor-main commits, 225 across all refs,
  245 historical Python blobs; canonical main has 60 commits.
- 14 donor-main files compared: 6 normalized-identical files; 88 shared function
  nodes, 37 AST-identical. Deleted/reverted behavior and unmerged refs inspected.
- Canonical import f185e2c already uses ddp_connectors paths.

Path shorthand (under each library's database_connectors/): PG =
postgres_connector.py, IFX = informix_connector.py, SQLS = sql_server_connector.py,
BASE = sql_connector.py, TYPES = sql_connector_utils.py, QUERY =
utils/postgres_connector_utils.py, ENUMS = utils/enums.py.

## Consolidation matrix

| Donor behavior/fix | Donor commit / file | Canonical equivalent | Classification | Implemented action |
|---|---|---|---|---|
| Literal column defaults | 1fe8371:PG | Imported f185e2c; removed 5b5f77c | PORT / ADAPT TO CANONICAL | Add keyword-only include_defaults=True for validated literals; default SQL remains unchanged |
| One composite partition PK | d409c82:PG with 4a75e1a ordinary-PK suppression | 5b5f77c restored ordinary PK, causing duplicate partition PKs | PORT / ADAPT TO CANONICAL | Suppress metadata PK only in copied partition columns; emit one explicit composite PK; preserve ordinary PKs |
| Partition identifier safety | 4904753:PG; guards removed 6a34e5d | bbda55d supplies exact quoting helper | PORT / ADAPT TO CANONICAL | Apply exact quoting to table/index/PK/partition DDL; preserve valid unusual identifiers |
| Parent/default/year/month partition operations | 4904753, d409c82:PG | f185e2c, AST-identical at main | ALREADY PRESENT / SUPERSEDED | Preserve date boundaries/naming/rollback/cleanup; adapt interactions above |
| Schema/table creation and optional indexes | 6569a36, 12a9cd0, 0db76c1, ab02de1, a37e7ac, c08dbab:PG | f185e2c; 7efe7e4 newer type handling | ALREADY PRESENT / SUPERSEDED | Keep canonical declarations/types and ordinary PKs |
| Index and PK management | 5c56393, 6066f4a:PG | f185e2c, AST-identical | ALREADY PRESENT / SUPERSEDED | Preserve |
| Index recreation/removal/analyze | 5998ac2:PG | e3b58d9, AST-identical | ALREADY PRESENT / SUPERSEDED | No duplicate lifecycle helpers |
| Index discovery | 5998ac2:PG/IFX/SQLS; 380a33f, a192c69, 96e8ac2:IFX | e3b58d9; stronger c5ee876 schema and 41ed201 IFX catalog handling | ALREADY PRESENT / SUPERSEDED | Keep canonical owner/schema resolution |
| Shared type maps | 8517748, 135df8f, 6569a36, 9d4a3e6, e8bc715, b1cebae, 8ce845d:TYPES | f185e2c plus canonical UI normalization/newer engines | ALREADY PRESENT / SUPERSEDED | Preserve newer type coverage |
| Byte/string conversion | 7e29fa4:TYPES | f185e2c, AST-identical | ALREADY PRESENT / SUPERSEDED | Lock conversion in baseline tests |
| Pandas type inference | 5cec9f2:QUERY | f185e2c, normalized-identical | ALREADY PRESENT / SUPERSEDED | Retain inference; declare imported dependencies |
| PostgreSQL JSON query builder/enums | 94bf584, b23a1fe, d5e1b5e, 6132901, a4231fe:PG/QUERY; 9f70300:ENUMS | f185e2c, normalized-identical utilities and build_query | ALREADY PRESENT / SUPERSEDED | Preserve |
| Parameterized extraction filters | cea5c86, 9766cdc:PG | f185e2c; stronger 95912ec JSONB/numeric support and bbda55d exact identifiers | ALREADY PRESENT / SUPERSEDED | Preserve newer filters |
| Paginated extraction | 7e29fa4, b423502, afc14b3, 2876fc3, 5f7f9ca, 71b913d:PG/IFX; e8bc715:SQLS | f185e2c, c5ee876 schema/API normalization | ALREADY PRESENT / SUPERSEDED | Preserve batch_size/legacy limit, qualified tables and borrowed cursors |
| Streaming | 9131ca4 superseded by 2ee8ba4:SQLS; 2106d2a:PG/IFX | f185e2c; 6e2eaf5/a099db8 PG date/time and cursor enhancements | ALREADY PRESENT / SUPERSEDED | Preserve; retry variants belong KAN-7 |
| CDC/latest rows/composite keys | 163c3ca, c01ef6d, 006e1a6, d1aea71, 420514f, 2547a72, 3683b9a:PG/IFX/SQLS | f185e2c/c5ee876; canonical PG also handles composite keys | ALREADY PRESENT / SUPERSEDED | Preserve stronger canonical CDC |
| Min/max | 0bc15c1, 0e625b7, 6a34e5d, 3327009:IFX/PG/SQLS | f185e2c plus qualified/exact identifiers | ALREADY PRESENT / SUPERSEDED | Preserve |
| Truncate/cleanup/base validation | 5843b2b, 4c9e211, d895537, 9d68f52:PG; 0f6eaf5:SQLS; e487ecc:BASE | f185e2c/c5ee876 | ALREADY PRESENT / SUPERSEDED | Preserve; recovery changes excluded |
| Schema/view/duplicate columns | 6569a36, cc004f5, d78d191:PG; 0835eb9:SQLS | f185e2c; 7430df9 UDT/search path, 9fce956/7a8c492 metadata diagnostics | ALREADY PRESENT / SUPERSEDED | Preserve; no unsupported removal of inherited view defaults |
| Informix locale/encoding/timeout/logging | f6283dd, ddfda16, 3876a04, 197d5b8, 996c0e0, a0dd2bb:IFX/SQLS | f185e2c | ALREADY PRESENT / SUPERSEDED | Keep Latin-1/UTF-8 choices and safe logs; earlier encoding experiments superseded |
| Driver/factory architecture | 92f72cd:SQLS; 562d398:factory | f185e2c; 5b5f77c Oracle, 5cf1e7a Mongo, 69bfa7c MySQL, 227cbdc Db2i, 8e65750 SFTP | ALREADY PRESENT / SUPERSEDED | Preserve seven databases plus SFTP, lazy factory and current errors |
| Retry/reconnect/pooling | 66c5855:BASE/IFX/SQLS/package init | Absent from main | OWNED BY ANOTHER CORE TICKET | KAN-7: no retry wrapper, backoff, ping, delta/stream reconnect or pooling change |
| Retired Informix JSON builder | 9f70300/5ff3d7a; removed 8896597:IFX/utils | Removed by design; platform uses its own rules builder | ALREADY PRESENT / SUPERSEDED | Do not resurrect |
| Retired DataFrame/engine/reflection APIs | 8bf9214:connector.py/BASE; 8896597:BASE/Oracle | Canonical raw-driver architecture | ALREADY PRESENT / SUPERSEDED | No compatibility wrappers; no current platform calls found |
| Retired PG SQLAlchemy builder/old index APIs | b23a1fe:QUERY/PG; 5c56393/d9c5149:PG | Surviving replacements at f185e2c/e3b58d9 | ALREADY PRESENT / SUPERSEDED | Preserve replacements |
| Runtime metadata and canonical docs | 90448a9:README.md; donor setup.py | Main missing direct imports, stale driver/Python/docs; runtime branch starts 9290671 | PORT / ADAPT TO CANONICAL | Reconcile packaging, driver declarations, neutral examples and install tests |
| Branding/client configuration | Donor README/setup and deployment examples | Not generic runtime behavior | CLIENT-SPECIFIC | Exclude client names/config/datasets/compatibility packages |
| Consumer integration | Platform 70d83b4:data_mart_utils.py, rules_service.py | Platform-owned | OWNED BY ANOTHER CORE TICKET | KAN-8: no API/Sync pin or source changes |

## Other references and compatibility decisions

Donor branch-only Db2/AS400 work (18a061b through 41b12ed) is superseded by the
canonical Db2i work (227cbdc, df9c5fb, 88e86d8, 4d68e5b). Informix SQLAlchemy
experiments on the donor sync-test branch and tag-only commits 46b0029,
193843d, d8da0e4, 62e7030 and 7d73e13 represent abandoned alternative designs.
No legacy engine/factory aliases are introduced.

The unmerged canonical package-runtime branch at fb21e0c was reviewed and its
packaging approach reconciled. It is not canonical main and is not a separate
ticket by virtue of being a branch.

Consumer evidence: deepkube-data-platform API/Sync pins 0.0.12; data-platform
API/Sync pins 0.0.18; a migration service pins 0.0.5.8. Five common contract
checks pass against both newer pinned releases; three applicable SQL/schema/byte
checks pass against 0.0.5.8. Factory support added after those tags remains in
canonical main. Unsupported factory types already raise ValueError on main;
older tags returned None. This pre-existing difference is recorded, not changed.

The partition caller supplies explicit key and partition column; metadata PKs
must not create a second constraint. Legacy subclass overrides of the three-
argument ordinary builder remain valid when defaults are not requested.
Literal default copying is opt-in because it can change target inserts.
Functions/sequences/source dialect expressions/custom casts are omitted, never
replayed as executable default SQL. Identifier quoting preserves exact spelling
and uses no legacy restrictive whitelist.

## Packaging and validation

Release target: ddp-connectors 0.2.0, independently installable and releasable.
The namespace remains ddp_connectors. Metadata requires Python 3.10+, declares
pandas/numpy/Mongo imports, replaces the unused cx_oracle
declaration with oracledb, and installs psycopg2-binary for the existing psycopg2
import. Driver coverage is retained. NumPy stays below 2 to support pandas 1.5
consumer environments; PyMongo stays below 4 to retain existing consumer pins.

On 2026-10-07, the dependency audit confirmed that the sole ddp-lib runtime import
was Oracle's serialize_if_needed helper. At the user's direction, the same
dict/list JSON serialization now lives in sql_connector_utils.py. The original
ddp_lib.utils function remains available to its existing consumers. No other
Oracle behavior changes. The package dependency, sibling CI checkout, credential
and dependency-reference input are removed.

scripts/smoke_install.py tests the connector wheel/sdist in clean environments
and asserts that ddp-lib is absent. It checks dependency consistency,
all imports (including lazy JDBC drivers), factory classes/settings and regression
contracts. The consumer constraint profile targets Python 3.10. Optional
test_postgres_live.py executes partition/default/index DDL against a configured
test PostgreSQL. No production database is used.

See RELEASING.md for system driver requirements and the release procedure, and
VALIDATION.md for executed checks and limitations.
