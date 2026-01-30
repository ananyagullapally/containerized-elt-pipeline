{{ config(materialized='view') }}

SELECT 
    c.customer_city, 
    SUM(i.price) as total_order_value
FROM {{ source('public', 'fact_orders') }} o
JOIN {{ source('public', 'dim_customers') }} c ON o.customer_id = c.customer_id
JOIN {{ source('public', 'fact_order_items') }} i ON o.order_id = i.order_id
GROUP BY c.customer_city
