CREATE OR REPLACE TABLE gold_top_products AS

SELECT 
    p.product_name,
    SUM(f.total_amount) AS revenue,

    ROUND(
        SUM(f.total_amount) * 100.0 /
        SUM(SUM(f.total_amount)) OVER(),
        2
    ) AS contribution_pct,

    RANK() OVER (
        ORDER BY SUM(f.total_amount) DESC
    ) AS rank

FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;

select * from gold_top_products limit 100;