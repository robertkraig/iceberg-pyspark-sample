SELECT
  c.customer_id,
  c.customer_name,
  SUM(o.total_amount) AS spend
FROM iceberg.sales.customers c
JOIN iceberg.sales.orders o
  ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY spend DESC
