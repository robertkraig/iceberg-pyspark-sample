# Multi-Vendor Ingestion Pattern (Iceberg Landing Zone)

This repo can scale from a single CSV demo to a multi-tenant ingestion pattern where each vendor's source DB is replicated into Iceberg and queried centrally.

## Goal

- Decouple analytics from slow/under-indexed vendor databases.
- Pull only required source tables into a local "landing zone".
- Standardize queries against Iceberg instead of vendor-specific systems.

## Recommended Data Flow

1. Extract from vendor DB (scheduled job).
2. Write raw snapshots/incrementals to landing storage (`S3`/MinIO), ideally as Parquet.
3. Register/merge into Iceberg tenant namespace.
4. Run normalized SQL from `sql/query/*.sql` against Iceberg.
5. Serve consumers via Trino (`jdbc:trino://localhost:8088/iceberg/sales` in this repo).

## Tenant Model

- Use one namespace per vendor/tenant.
- Pattern:
  - `iceberg.<tenant>.orders`
  - `iceberg.<tenant>.order_items`
  - `iceberg.<tenant>.customers`
- Keep table names consistent across tenants whenever possible.

## What to Automate

## 1) Vendor Onboarding Config

Create a machine-readable config per vendor (YAML/JSON) with:

- Connection secret reference (never commit secrets).
- Tenant id/namespace name.
- Source table list.
- Incremental key per table (`updated_at`, sequence id, etc.).
- Extract SQL template or table mapping.

## 2) DDL Capture + Table Provisioning

- Pull source metadata (columns/types/nullability).
- Map vendor types to Spark/Iceberg-compatible types.
- Generate SQL files in `sql/load/` (or vendor-specific folder) for:
  - `CREATE NAMESPACE IF NOT EXISTS iceberg.<tenant>`
  - `CREATE TABLE iceberg.<tenant>.<table> (...) USING iceberg PARTITIONED BY (...)`
- Keep generated DDL and PySpark schemas aligned.

## 3) Incremental Ingestion Jobs

- For each table, extract changed rows since last watermark.
- Write landed files to a deterministic path, e.g.:
  - `landing/<tenant>/<table>/ingest_date=YYYY-MM-DD/part-*.parquet`
- Load/merge into Iceberg table.
- Persist watermark/state per tenant+table.

## 4) Query Normalization

- Keep canonical analytics SQL in files (as this repo does in `sql/query/`).
- Replace vendor DB references with `iceberg.<tenant>.<table>`.
- Standardize dialect to Spark SQL/Trino SQL; avoid source-specific functions.

## 5) Validation and Observability

- Per load, record:
  - extracted row count
  - loaded row count
  - min/max incremental key
  - load timestamp
- Add parity checks for key aggregates against source when onboarding.

## Partitioning / Performance Defaults

- Partition by time filters used most often (`days(order_date)` / `months(order_date)`).
- Bucket high-cardinality join keys if joins are heavy.
- Write sorted data on common filter/join keys.
- Periodically compact small files.

## Suggested Folder Convention

```text
docs/
  multi-vendor-ingestion-pattern.md
sql/
  load/
  query/
  vendors/
    <tenant_a>/
      load/
      query/
state/
  watermarks/
```

## Practical Rollout Plan

1. Start with one vendor and 3-5 high-value tables.
2. Implement full incremental loop + validation.
3. Migrate a small query pack from source DB to Iceberg.
4. Tune partitioning/bucketing after real query profiling.
5. Repeat onboarding template for additional vendors.

## Repo-Specific Notes

- Current local stack is MinIO + Iceberg REST + Trino via `make up`.
- Spark scripts already read SQL from files (`sql/load/*.sql`, `sql/query/*.sql`).
- Current demo namespace is `iceberg.sales`; multi-tenant rollout should use `iceberg.<tenant>`.
