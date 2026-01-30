{{ config(materialized='view') }}

SELECT 
    p.product_category_name,
    COUNT(i.order_id) as total_sales_count,
    SUM(i.price) as total_revenue,
    SUM(i.freight_value) as total_shipping_costs,
    ROUND((SUM(i.price) / (SUM(i.price) + SUM(i.freight_value)))::numeric, 3) as margin_ratio
FROM dim_products p
JOIN fact_order_items i ON p.product_id = i.product_id
GROUP BY 1
HAVING COUNT(i.order_id) > 100
ORDER BY margin_ratio DESC
