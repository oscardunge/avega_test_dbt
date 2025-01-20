






  MERGE INTO `primeval-rune-447712-f5`.`futurewaterlevel`.`events` AS target
  USING None AS source
  ON target.id = source.id  -- Make absolutely sure this is correct
  WHEN MATCHED THEN UPDATE SET
      -- List all columns to update explicitly
      target.event_name = source.event_name,
      target.event_date = source.event_date,
      target.value = source.value
  WHEN NOT MATCHED THEN INSERT (id, event_name, event_date, value) -- List all columns to insert
  VALUES (source.id, source.event_name, source.event_date, source.value);
