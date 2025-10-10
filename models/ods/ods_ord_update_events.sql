{{ config(materialized='table') }}

WITH stg_data AS (
    SELECT
        simpleJSONExtractString(message_payload, 'agentId') AS agentId,
        simpleJSONExtractString(message_payload, 'trackingPointId') AS trackingPointId,
        simpleJSONExtractString(message_payload, 'createdAt') AS createdAt,
        JSONExtractArrayRaw(message_payload, 'correlationIds') AS correlationIds,
        replace(arrayElement(correlationIds, 2), '"', '') AS order_id,
        simpleJSONExtractRaw(message_payload, 'payload') AS payload,
        simpleJSONExtractString(payload, 'disChannel') AS disChannel,
        simpleJSONExtractString(payload, 'fname') AS attribute,
        simpleJSONExtractString(payload, 'valueNewDesc') AS zzHeaderStatusTxt

    FROM {{ ref('stg_messages') }}
    WHERE agentId = 'SAP_CRM' and trackingPointId = 'CRM_ZND_UPDATED' and disChannel !='10' and attribute = 'STAT'
)

SELECT
    agentId,
    trackingPointId,
    createdAt,
    order_id,
    disChannel,
    attribute,
    zzHeaderStatusTxt

FROM stg_data
WHERE agentId = 'SAP_CRM' and trackingPointId = 'CRM_ZND_UPDATED' and disChannel !='10' and attribute = 'STAT'


