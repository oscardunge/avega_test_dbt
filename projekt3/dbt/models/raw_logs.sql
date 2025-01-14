select * 
from {{ source('csv_data', 'raw_logs') }}
