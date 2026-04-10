SELECT
  c.customer_id,
  c.customer_name,
  c.region,
  COUNT(*) AS purchase_events,
  SUM(e.amount) AS event_revenue,
  SUM(o.total_amount) AS order_revenue
FROM iceberg.sales.customers c
JOIN iceberg.sales.events e ON c.customer_id = e.customer_id
LEFT JOIN iceberg.sales.orders o ON e.order_id = o.order_id
WHERE e.event_type = 'PURCHASE' AND e.event_year = 2026
GROUP BY c.customer_id, c.customer_name, c.region
ORDER BY event_revenue DESC
