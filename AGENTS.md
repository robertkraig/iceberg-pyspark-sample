# AGENTS Notes

## Repo Purpose
- Local Iceberg demo stack: MinIO + Iceberg REST + Trino, with PySpark scripts for load/query.
- Primary scripts: `load.py` (ingest CSV -> Iceberg) and `query.py` (run SQL queries).

## Commands That Matter
- Start core services: `make up`
- Stop services cleanly (also removes orphan containers): `make down`
- Local Python run: `make load` then `make query`
- Optional Spark cluster mode: `make spark-up`, `make cluster-load`, `make cluster-query`
- Trino helpers: `make trino-up`, `make trino-logs`, `make trino-shell`

## Source Of Truth For SQL
- Do not inline business SQL in Python unless necessary.
- Load-side DDL/DML lives in `sql/load/*.sql`; `load.py` executes these files in numeric order.
- Query-side SQL lives in `sql/query/*.sql`; `query.py` reads and executes those files.

## Important Wiring / Naming
- Iceberg catalog name is `iceberg` in both Spark and Trino.
- Table names should be fully qualified as `iceberg.sales.<table>` in Spark SQL files.
- Trino JDBC endpoint is `localhost:8088`; Spark master UI/worker UI are `18080`/`18081`.

## Gotchas
- `load.py` enforces CSV schemas via `StructType` **and** creates tables via SQL files; keep both definitions in sync when changing columns/types.
- If Docker Compose warns about old containers after service changes, use `make down` then `make up`.
- MinIO/Iceberg local credentials are hardcoded for dev (`admin` / `password`, region `us-east-1`).

## Dependency / Runtime
- Python via `uv`; project dependency is pinned to `pyspark==3.5.1` in `pyproject.toml`.
- There is no configured test/lint pipeline in this repo; validate changes by running `make load` and `make query`.
