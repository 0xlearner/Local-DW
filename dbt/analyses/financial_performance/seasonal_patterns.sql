SELECT
    EXTRACT(MONTH FROM month_start_date) AS month_num,
    TO_CHAR(month_start_date, 'Month') AS month_name,
    AVG(seasonal_index) AS avg_seasonal_index,
    COUNT(DISTINCT CASE WHEN seasonality = 'High Season' THEN listing_id END) AS high_season_listings,
    COUNT(DISTINCT CASE WHEN seasonality = 'Low Season' THEN listing_id END) AS low_season_listings,
    AVG(occupancy_rate) AS avg_occupancy
FROM {{ ref('fct_financial_performance_metrics') }}
GROUP BY EXTRACT(MONTH FROM month_start_date), TO_CHAR(month_start_date, 'Month')
ORDER BY month_num
