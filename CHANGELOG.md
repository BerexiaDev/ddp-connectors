# Changelog

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
