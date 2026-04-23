create or replace table gold_category_performance as 

SELECT 
    d.year,
    d.month,
    p.category,
    SUM(f.total_amount) AS revenue

FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_key = d.date_key

GROUP BY d.year, d.month, p.category
ORDER BY d.year, d.month;