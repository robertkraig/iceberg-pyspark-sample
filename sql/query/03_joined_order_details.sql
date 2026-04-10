SELECT
  e.event_date,
  e.event_year,
  e.event_month,
  e.event_day,
  e.order_id,
  c.customer_name,
  c.region,
  p.product_name,
  oi.qty,
  oi.unit_price,
  e.quantity AS event_qty,
  e.amount AS event_amount,
  (oi.qty * oi.unit_price) AS order_line_total,
  o.status
FROM iceberg.sales.events e
JOIN iceberg.sales.orders o
  ON e.order_id = o.order_id
 AND e.customer_id = o.customer_id
 AND e.event_date = o.order_date
JOIN iceberg.sales.customers c
  ON e.customer_id = c.customer_id
JOIN iceberg.sales.order_items oi
  ON e.order_id = oi.order_id
 AND e.event_date = oi.order_date
 AND e.product_id = oi.product_id
JOIN iceberg.sales.products p
  ON e.product_id = p.product_id
WHERE e.event_type = 'PURCHASE'
  AND e.event_year = 2026
  AND e.event_month IN (1, 2)
ORDER BY e.event_date, e.order_id, p.product_name
