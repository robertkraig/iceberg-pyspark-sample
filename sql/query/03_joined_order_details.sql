SELECT
  o.order_date,
  o.order_id,
  c.customer_name,
  c.region,
  p.product_name,
  oi.qty,
  oi.unit_price,
  (oi.qty * oi.unit_price) AS line_total,
  o.status
FROM iceberg.sales.orders o
JOIN iceberg.sales.customers c
  ON o.customer_id = c.customer_id
JOIN iceberg.sales.order_items oi
  ON o.order_id = oi.order_id
 AND o.order_date = oi.order_date
JOIN iceberg.sales.products p
  ON oi.product_id = p.product_id
WHERE o.order_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28'
ORDER BY o.order_date, o.order_id, p.product_name
