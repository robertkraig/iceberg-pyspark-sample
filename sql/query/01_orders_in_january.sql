SELECT order_id, customer_id, order_date, status, total_amount
FROM iceberg.sales.orders
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
ORDER BY order_date, order_id
