{{ config(materialized='table') }}

WITH stg_data AS (
    SELECT
        simpleJSONExtractString(message_payload, 'agentId') AS agentId,
        simpleJSONExtractString(message_payload, 'trackingPointId') AS trackingPointId,
        simpleJSONExtractString(message_payload, 'createdAt') AS createdAt,
        simpleJSONExtractRaw(message_payload, 'payload') AS payload,
        simpleJSONExtractString(payload, 'poNumberUc') AS order_id,
        simpleJSONExtractString(payload, 'zzPayStatusTxt') AS zzPayStatusTxt,
        simpleJSONExtractString(payload, 'zzHeaderStatusTxt') AS zzHeaderStatusTxt,
        simpleJSONExtractString(payload, 'zzStore') AS zzStore,
        simpleJSONExtractString(payload, 'zzPaymentMethod') AS zzPaymentMethod,
        simpleJSONExtractString(payload, 'disChannel') AS disChannel,
        simpleJSONExtractString(payload, 'zzShipCond') AS zzShipCond,
        simpleJSONExtractString(payload, 'zzCity1') AS zzCity1,
        simpleJSONExtractFloat(payload, 'zzSum') AS zzSum,
        simpleJSONExtractString(payload, 'zzCustType') AS zzCustType,
        simpleJSONExtractString(payload, 'zzDlvIntDesc') AS zzDlvIntDesc
    FROM {{ ref('stg_messages') }}
    WHERE agentId = 'SAP_CRM' and trackingPointId = 'CRM_ZND_CREATED' and disChannel !='10'
)

SELECT
    agentId,
    trackingPointId,
    createdAt,
    order_id,
    zzPayStatusTxt,
    zzHeaderStatusTxt,
    zzStore,
    zzPaymentMethod,
    disChannel,
    zzShipCond,
    zzCity1,
    zzSum,
    zzCustType,
    zzDlvIntDesc
FROM stg_data
WHERE agentId = 'SAP_CRM' and trackingPointId = 'CRM_ZND_CREATED' and disChannel !='10'


