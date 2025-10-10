{{ config(materialized='view') }}

WITH dds_data AS (
    SELECT
    DATE(createdAt) AS dates,
    COUNT(CASE WHEN zzHeaderStatusTxt = 'Завершен' THEN order_id END) AS completed_orders_count,
    SUM(CASE WHEN zzHeaderStatusTxt = 'Завершен' THEN zzSum ELSE 0 END) AS completed_orders_sum,
    COUNT(CASE WHEN zzHeaderStatusTxt = 'Отменен' THEN order_id END) AS cancelled_orders_count,
    SUM(CASE WHEN zzHeaderStatusTxt = 'Отменен' THEN zzSum ELSE 0 END) AS cancelled_orders_sum
    FROM {{ ref('dds_order_fact') }}
    WHERE YEAR(createdAt) = 2025
    GROUP BY DATE(createdAt)
)

SELECT
        dates,
        completed_orders_count,
        completed_orders_sum,
        cancelled_orders_count,
        cancelled_orders_sum
FROM dds_data


