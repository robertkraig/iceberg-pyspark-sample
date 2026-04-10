SELECT
  oi.order_id,
  oi.order_date,
  SUM(oi.qty * oi.unit_price) AS line_total
FROM iceberg.sales.order_items oi
WHERE oi.order_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28'
GROUP BY oi.order_id, oi.order_date
ORDER BY oi.order_date, oi.order_id
