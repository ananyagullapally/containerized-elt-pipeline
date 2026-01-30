{{ config(materialized='view') }}

SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp::TIMESTAMP) as report_month,
    SUM(i.price) as current_month_revenue
FROM {{ source('public', 'fact_orders') }} o
JOIN {{ source('public', 'fact_order_items') }} i ON o.order_id = i.order_id
GROUP BY 1
