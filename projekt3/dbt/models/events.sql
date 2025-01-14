
    SELECT event_id, event_type, event_details
    FROM {{ source('csv_data', 'events') }}
    