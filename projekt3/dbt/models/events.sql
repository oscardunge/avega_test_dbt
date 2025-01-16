{{ config(materialized='table') }}

MERGE INTO project_id.dataset.events AS target
USING (
    -- Your previous query with deduplication logic
    SELECT (ARRAY_AGG(e ORDER BY _PARTITIONTIME ASC, _FILETIME ASC LIMIT 1))[OFFSET(0)].*
    FROM (
        SELECT *
        FROM {{ external_source('csv_data', 'csv_data_events.csv') }} AS ext_events
    ) AS e
    GROUP BY project_id, dataset, another_column
) AS source
ON target.project_id = source.project_id
AND target.dataset = source.dataset
