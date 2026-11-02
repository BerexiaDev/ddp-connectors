# Changelog

## 0.2.3 — 2026-11-02

- Integrate the Informix connection-test query and readable ping errors with
  the shared retry/cleanup behavior, including Oracle missing-schema errors.
- Retain main's Oracle schema discovery fix: columns in multiple indexes or
  constraints appear once, with owner-scoped metadata.
- Resolve the older Informix branch against the strict Latin-1/UTF-8 encoding
  contract from 0.2.2; ambiguous locale guessing and its 0.1.3 packaging are
  superseded. No existing release tag is changed.

## 0.2.2 — 2026-11-02

- Recognize Oracle TIMESTAMP precision 0–9, including both timezone variants,
  using the existing PostgreSQL TIMESTAMP/TIMESTAMPTZ declarations. Precision
  beyond microseconds is not preserved; existing TEXT columns require migration.
- Bind Informix text through SQL_CHAR using the configured locale: Latin-1
  (`819`, `8859-1`) or UTF-8 (`utf8`, `57372`). Decode character columns using
  the same codec; reject unsupported locales and unrepresentable text explicitly.
- Add unit and optional real-driver regressions for schema discovery, PostgreSQL
  DDL, fractional timestamps, and Latin-1/UTF-8 parameter round trips.

## 0.2.1

- Add bounded connection retries with configurable backoff across database
  connectors, using driver-specific checks for temporary connection failures.
- Retry safe reads on owned connections and stop retrying streamed reads after
  the first result; preserve caller-owned cursors and their session state.
- Restrict parameter-binding fallbacks to known unsupported driver operations.
- Validate retry settings in the connector factory before creating connectors.

## 0.2.0

- Produce one composite primary key for partitioned PostgreSQL tables without
  changing ordinary table primary keys or mutating caller metadata.
- Quote exact PostgreSQL table, schema, column, index and partition identifiers.
- Restore literal defaults through `include_defaults=True` on PostgreSQL table
  builders. Default calls continue to omit defaults; function expressions,
  source dialect expressions and custom type casts are omitted.
- Preserve legacy builder override signatures when the option is unused.
- Keep Oracle's JSON serializer in the connector utilities, preserving its
  dict/list encoding and scalar/error behavior. Remove the ddp-lib dependency.
- Declare all imported runtime dependencies and use the actual `oracledb` driver.
  Retain all canonical engines and SFTP.
- Build and test the package independently; CI needs only this repository's checkout.
- Correct Python metadata to >=3.10 and add contract/regression/install tests.

This is a release candidate in source until tagged and published through the
normal release process. SQL recovery is KAN-7; consumer adoption is KAN-8.
