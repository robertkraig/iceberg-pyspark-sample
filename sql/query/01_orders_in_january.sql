SELECT
  event_year,
  event_month,
  event_day,
  event_type,
  COUNT(*) AS event_count,
  SUM(COALESCE(amount, 0)) AS total_amount
FROM iceberg.sales.events
WHERE event_year = 2026
  AND event_month IN (1, 2)
GROUP BY event_year, event_month, event_day, event_type
ORDER BY event_year, event_month, event_day, event_type
