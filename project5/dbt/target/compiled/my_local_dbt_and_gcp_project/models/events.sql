






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
