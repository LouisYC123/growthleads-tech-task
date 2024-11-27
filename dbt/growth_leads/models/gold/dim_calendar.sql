-- TODO: dynamically generate the date range based on the min/max dates in table
WITH RECURSIVE date_range AS (
    SELECT CAST('2022-01-01' AS DATE) AS calendar_date
    UNION ALL
    SELECT (calendar_date + INTERVAL '1 day')::DATE
    FROM date_range
    WHERE calendar_date < '2027-12-31'
)
SELECT
    calendar_date,
    EXTRACT(YEAR FROM calendar_date) AS year,
    EXTRACT(MONTH FROM calendar_date) AS month,
    EXTRACT(DAY FROM calendar_date) AS day,
    EXTRACT(DOW FROM calendar_date) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM calendar_date) IN (6, 0) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
