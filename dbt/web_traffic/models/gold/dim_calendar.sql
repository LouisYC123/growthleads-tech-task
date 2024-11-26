WITH RECURSIVE date_range AS (
    SELECT CAST('2020-01-01' AS DATE) AS calendar_date
    UNION ALL
    SELECT (calendar_date + INTERVAL '1 day')::DATE
    FROM date_range
    WHERE calendar_date < '2030-12-31'
)
SELECT
    calendar_date,
    EXTRACT(YEAR FROM calendar_date) AS year,
    EXTRACT(MONTH FROM calendar_date) AS month,
    EXTRACT(DAY FROM calendar_date) AS day,
    EXTRACT(DOW FROM calendar_date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM calendar_date) IN (6, 0) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
