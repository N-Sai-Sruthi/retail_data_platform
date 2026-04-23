create or replace table gold_avg_order_trend as 

SELECT 
    d.year,
    d.month,
    SUM(f.total_amount) / COUNT(f.order_id) AS avg_order_value

FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key

GROUP BY d.year, d.month
ORDER BY d.year, d.month;

select * from gold_avg_order_trend;