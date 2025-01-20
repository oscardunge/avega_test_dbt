-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `primeval-rune-447712-f5`.`futurewaterlevel`.`events` as DBT_INTERNAL_DEST
        using (






  -- dbt will automatically generate the MERGE logic here
  SELECT *
  FROM (
    SELECT
      id,
      event_name,
      event_date,
      value
    FROM (
      SELECT
        id,
        event_name,
        event_date,
        value,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_date DESC) AS row_num
      FROM `primeval-rune-447712-f5.futurewaterlevel.staging_csv_data_events_20250120`
    ) AS subquery
    WHERE row_num = 1
  ) AS target

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
            )

    
    when matched then update set
        `id` = DBT_INTERNAL_SOURCE.`id`,`event_name` = DBT_INTERNAL_SOURCE.`event_name`,`event_date` = DBT_INTERNAL_SOURCE.`event_date`,`value` = DBT_INTERNAL_SOURCE.`value`
    

    when not matched then insert
        (`id`, `event_name`, `event_date`, `value`)
    values
        (`id`, `event_name`, `event_date`, `value`)


    