CREATE TABLE iceberg.sales.events (
  event_id BIGINT,
  customer_id BIGINT,
  product_id BIGINT,
  order_id BIGINT,
  session_id STRING,
  event_type STRING,
  event_ts TIMESTAMP,
  event_date DATE,
  event_year INT,
  event_month INT,
  event_day INT,
  quantity INT,
  amount DECIMAL(12,2)
) USING iceberg
PARTITIONED BY (event_year, event_month, event_day, bucket(16, customer_id))
