CREATE TABLE iceberg.sales.products (
  product_id BIGINT,
  product_name STRING,
  category STRING,
  price DECIMAL(10,2),
  updated_at TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket(8, product_id))
