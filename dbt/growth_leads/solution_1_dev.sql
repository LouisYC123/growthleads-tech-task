WITH temp1 AS (
    SELECT 
        te.* 
    FROM 
        silver.traffic_events te 
),
daily_totals AS (
    SELECT
        event_date,
        marketing_source_id,
        source,
        SUM(clicks) AS clicks,
        SUM(raw_earnings) AS raw_earnings,
        SUM(total_earnings) AS total_earnings
    FROM 
        temp1
    GROUP BY 
        event_date, marketing_source_id, source
),
scrapers_totals AS ( 
    SELECT 
        event_date,
        marketing_source_id,
        source,
        clicks,
        raw_earnings,
        total_earnings,
        COALESCE(
            total_earnings - LAG(total_earnings) OVER (
                PARTITION BY marketing_source_id
                ORDER BY event_date
            ),
            0
        ) AS total_commission,
        CAST(NULL AS NUMERIC) AS add_amount,          
        CAST(NULL AS BOOLEAN) AS has_plus_clicks,     
        CAST(NULL AS NUMERIC) AS clicks_multiplier  
    FROM 
        daily_totals
    WHERE 
        source = 'scrapers'
),
non_scrapers_totals AS (
    SELECT 
        dt.event_date,
        dt.marketing_source_id,
        dt.source,
        dt.clicks,
        dt.raw_earnings,
        dt.total_earnings,
        CASE 
            WHEN d.deal_type = 'default' THEN dt.raw_earnings
            WHEN d.has_plus_clicks THEN 
                COALESCE(dt.raw_earnings, 0) 
                + COALESCE(d.add_amount, 0) 
                + (COALESCE(dt.clicks, 0) * COALESCE(d.clicks_multiplier, 0))
            ELSE 
                dt.raw_earnings + d.add_amount
        END AS total_commission,
        d.add_amount::NUMERIC AS add_amount,         
        d.has_plus_clicks::BOOLEAN AS has_plus_clicks,
        d.clicks_multiplier::NUMERIC AS clicks_multiplier 
    FROM 
        daily_totals dt
        JOIN silver.marketing_sources ms 
            ON dt.marketing_source_id = ms.marketing_source_id
        JOIN silver.deals d 
            ON ms.deal_id = d.deal_id
    WHERE 
        dt.source <> 'scrapers'
)
SELECT 
    event_date,
    marketing_source_id,
    source,
    clicks,
    raw_earnings,
    total_earnings,
    total_commission,
    add_amount,
    has_plus_clicks,
    clicks_multiplier
FROM 
    scrapers_totals
UNION ALL
SELECT 
    event_date,
    marketing_source_id,
    source,
    clicks,
    raw_earnings,
    total_earnings,
    total_commission,
    add_amount,
    has_plus_clicks,
    clicks_multiplier
FROM 
    non_scrapers_totals
