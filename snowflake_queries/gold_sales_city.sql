CREATE OR REPLACE TABLE gold_sales_city AS
SELECT 
    c.city,
    SUM(f.total_amount) AS revenue,

    RANK() OVER (
        ORDER BY SUM(f.total_amount) DESC
    ) AS city_rank

FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.city
ORDER BY revenue DESC;

select * from gold_sales_city; 