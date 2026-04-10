SELECT
  e.event_date,
  e.event_id,
  c.customer_name,
  e.event_type,
  p.product_name,
  e.quantity,
  e.amount
FROM iceberg.sales.events e
JOIN iceberg.sales.customers c
  ON e.customer_id = c.customer_id
LEFT JOIN iceberg.sales.products p
  ON e.product_id = p.product_id
WHERE e.event_year = 2026
  AND e.event_month = 1
ORDER BY e.event_date, e.event_id
