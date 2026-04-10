# Local Iceberg + PySpark CSV Demo

## 1) Start Iceberg REST + MinIO

```bash
docker compose -f compose.yml up -d
```

This starts:
- Iceberg REST catalog on `http://localhost:8181`
- MinIO API on `http://localhost:9000`
- MinIO Console on `http://localhost:9001` (user: `admin`, pass: `password`)
- Trino on `http://localhost:8088`

Optional Spark cluster profile:
- Spark Master UI on `http://localhost:18080`
- Spark Worker UI on `http://localhost:18081`

## 2) Create a virtual environment with uv

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e .
```

## 3) Load CSV files into Iceberg tables

```bash
python load.py
```

`load.py` reads DDL/DML from `sql/load/*.sql`.

Tables created in catalog `iceberg`, namespace `sales`:
- `iceberg.sales.customers`
- `iceberg.sales.products`
- `iceberg.sales.orders`
- `iceberg.sales.order_items`
- `iceberg.sales.events`

`events` is partitioned by `event_year`, `event_month`, `event_day` (+ bucket on `customer_id`) and query samples join events to customer/product/order data.

## 4) Run SQL queries

```bash
python query.py
```

`query.py` reads query text from `sql/query/*.sql`.

## 5) Stop local services

```bash
docker compose -f compose.yml down
```

## Run on Spark cluster (optional)

```bash
make spark-up
make cluster-load
make cluster-query
```

## Connect from JetBrains via JDBC (Trino)

1. Start services:

```bash
make up
```

2. In JetBrains Database tool, create a Trino datasource with:
- URL: `jdbc:trino://localhost:8088/iceberg/sales`
- User: `trino`
- Password: (blank)

3. Query Iceberg tables with fully qualified names, for example:

```sql
SELECT * FROM sales.orders LIMIT 10;
```

`load.py` and `query.py` support these env vars for endpoints/credentials:
- `ICEBERG_REST_URI`
- `ICEBERG_S3_ENDPOINT`
- `ICEBERG_S3_REGION`
- `ICEBERG_S3_ACCESS_KEY`
- `ICEBERG_S3_SECRET_KEY`
