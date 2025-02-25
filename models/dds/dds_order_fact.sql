{{ config(materialized='table') }}

WITH ods_data AS (
    SELECT
        cr.order_id AS order_id,
        toDateTime(cr.createdAt, 3, 'UTC')  AS createdAt,
        cr.zzPayStatusTxt AS zzPayStatusTxt,
        cr.zzPaymentMethod AS zzPaymentMethod,
        cr.zzStore AS zzStore,
        cr.disChannel AS disChannel,
        cr.zzShipCond AS zzShipCond,
        cr.zzCity1 AS zzCity1,
        cr.zzSum AS zzSum,
        cr.zzCustType AS zzCustType,
        cr.zzDlvIntDesc AS zzDlvIntDesc,
        toDateTime(up.createdAt, 3, 'UTC') AS updatedAT,

       CASE
       WHEN updatedAT > createdAt THEN up.zzHeaderStatusTxt
       ELSE cr.zzHeaderStatusTxt
       END AS zzHeaderStatusTxt,

       ROW_NUMBER() OVER(PARTITION BY cr.order_id ORDER BY up.createdAt DESC) AS rn

    FROM {{ ref('ods_ord_create_events') }} cr JOIN  {{ ref('ods_ord_update_events') }} up ON cr.order_id = up.order_id
)

SELECT
order_id AS order_id,
        createdAt,
        zzPayStatusTxt,
        zzHeaderStatusTxt,
        zzPaymentMethod,
        zzStore,
        disChannel,
        zzShipCond,
        zzCity1,
        zzSum,
        zzCustType,
        zzDlvIntDesc,
        updatedAT
FROM ods_data
WHERE rn = 1


