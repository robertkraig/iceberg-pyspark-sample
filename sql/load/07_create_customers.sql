CREATE TABLE iceberg.sales.customers (
  customer_id BIGINT,
  customer_name STRING,
  signup_date DATE,
  region STRING
) USING iceberg
PARTITIONED BY (days(signup_date))
