CREATE TABLE iceberg.sales.order_items (
  order_item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  order_date DATE,
  qty INT,
  unit_price DECIMAL(10,2)
) USING iceberg
PARTITIONED BY (months(order_date), bucket(16, order_id))
