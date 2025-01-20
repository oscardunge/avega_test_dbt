{{ config(materialized='incremental', unique_key='id') }}

{% set staging_table_name = 'staging_csv_data_event_' ~ run_started_at.strftime("%Y%m%d") %}
{% set full_staging_table_name = target.project ~ '.' ~ target.dataset ~ '.' ~ staging_table_name %}
{% set staging_table = adapter.get_relation(database=target.project, schema=target.dataset, identifier=staging_table_name) %}

{% if is_incremental() %}
  MERGE INTO {{ this }} AS target
  USING {{ staging_table }} AS source
  ON target.id = source.id  -- Make absolutely sure this is correct
  WHEN MATCHED THEN UPDATE SET
      -- List all columns to update explicitly
      target.event_name = source.event_name,
      target.event_date = source.event_date,
      target.value = source.value
  WHEN NOT MATCHED THEN INSERT (id, event_name, event_date, value) -- List all columns to insert
  VALUES (source.id, source.event_name, source.event_date, source.value);
{% else %}
  SELECT * FROM {{ staging_table }};
{% endif %}