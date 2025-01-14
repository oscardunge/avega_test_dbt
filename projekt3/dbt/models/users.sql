
    SELECT user_id, user_name FROM {{ source('csv_data', 'users') }} 
    