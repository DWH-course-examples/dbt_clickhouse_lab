{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT
        _airbyte_raw_id AS airbyte_message_id,
        _airbyte_extracted_at AS airbyte_extracted_at,
        simpleJSONExtractRaw(_airbyte_data, 'ID') AS message_id,
        simpleJSONExtractString(_airbyte_data, 'PAYLOAD') AS message_payload,
        simpleJSONExtractString(_airbyte_data, 'RECEIVED_AT') AS message_received_at
    FROM {{ source('oracle_source', 'INCOMING_MESSAGES') }}
)

SELECT
    airbyte_message_id,
    airbyte_extracted_at,
    message_id,
    message_payload,
    message_received_at
FROM raw_data