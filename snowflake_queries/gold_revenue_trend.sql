CREATE OR REPLACE TABLE gold_revenue_trend AS

SELECT 
    d.year,
    d.month,
    SUM(f.total_amount) AS revenue,
    
    LAG(SUM(f.total_amount)) OVER (
        ORDER BY d.year, d.month
    ) AS prev_month_revenue,

    ROUND(
        (SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month))
        * 100.0 /
        NULLIF(LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month), 0),
        2
    ) AS growth_pct

FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month;


select * from gold_revenue_trend;