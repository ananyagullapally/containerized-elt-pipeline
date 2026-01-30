{{ config(materialized='view') }}

SELECT 
    p.product_category_name,
    COUNT(o.order_id) as total_sales_count,
    SUM(i.price) as total_revenue,
    SUM(i.freight_value) as total_shipping_costs,
    ROUND((SUM(i.price) / NULLIF(SUM(i.price) + SUM(i.freight_value), 0))::numeric, 3) as margin_ratio
FROM {{ source('public', 'dim_products') }} p
JOIN {{ source('public', 'fact_order_items') }} i ON p.product_id = i.product_id
JOIN {{ source('public', 'fact_orders') }} o ON i.order_id = o.order_id
GROUP BY p.product_category_name
HAVING COUNT(o.order_id) > 100
