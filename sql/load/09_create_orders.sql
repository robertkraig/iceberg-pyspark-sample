CREATE TABLE iceberg.sales.orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_ts TIMESTAMP,
  order_date DATE,
  status STRING,
  total_amount DECIMAL(12,2)
) USING iceberg
PARTITIONED BY (months(order_date), bucket(16, customer_id))
