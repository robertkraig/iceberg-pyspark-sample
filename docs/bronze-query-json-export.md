# Vendor DB -> Bronze -> Query -> JSON Export

This pattern treats Iceberg as a Bronze cache for vendor databases, then runs PySpark SQL over Bronze tables and exports result sets to JSON for downstream systems.

## Architecture

1. Pull vendor tables from source systems into landing files (Parquet preferred, NDJSON acceptable).
2. Load landing data into Iceberg Bronze tables (multi-tenant namespaces if needed).
3. Run analytics/joins from SQL files via PySpark (`sql/query/*.sql`).
4. Export query results as JSON payloads consumed by existing application flows.

## Why This Works

- Decouples heavy queries from vendor DB performance limits.
- Keeps source extraction and analytics compute separate.
- Reuses the existing SQL-file workflow already in this repo.
- Produces JSON outputs compatible with API/event pipelines.

## Bronze Table Guidance

- Keep Bronze close to source shape (minimal transformations).
- Add ingestion metadata columns when possible: `tenant_id`, `batch_id`, `ingested_at`.
- Partition by dominant time filters (for events, `event_year/event_month/event_day` is a good default).
- Bucket high-cardinality join keys when joins are frequent.

## Querying Bronze in This Repo

- Query text lives in `sql/query/*.sql`.
- `query.py` reads those SQL files and executes them with Spark against Iceberg tables.
- Keep SQL fully qualified with catalog/namespace/table (e.g., `iceberg.sales.events`).

## Exporting Query Results to JSON

Use Spark DataFrame writer for scalable JSON export:

```python
from pathlib import Path

sql_path = Path("sql/query/03_joined_order_details.sql")
sql_text = sql_path.read_text(encoding="utf-8")

df = spark.sql(sql_text)

# NDJSON output (one JSON object per line)
df.write.mode("overwrite").json("exports/joined_order_details")
```

For a single local JSON artifact (small result sets only):

```python
rows = [r.asDict(recursive=True) for r in df.collect()]
```

Prefer distributed `DataFrame.write.json(...)` for large outputs.

## Operational Notes

- Use Bronze as the analytics execution layer; keep Silver/Gold elsewhere if your platform already handles that.
- Keep query logic in SQL files, not embedded Python strings.
- Validate each export with row counts and batch metadata before publishing.
