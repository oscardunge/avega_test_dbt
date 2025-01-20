{{ config(materialized='incremental', unique_key='id') }}

{% set staging_table_name = 'staging_csv_data_events_' ~ run_started_at.strftime("%Y%m%d") %}
{% set full_staging_table_name = target.project ~ '.' ~ target.dataset ~ '.' ~ staging_table_name %}
{% set this_table_name = target.project ~ '.' ~ target.dataset ~ '.' ~ this.identifier %}

{% if is_incremental() %}
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
      FROM `{{ full_staging_table_name }}`
    ) AS subquery
    WHERE row_num = 1
  ) AS target
{% else %}
  CREATE OR REPLACE TABLE {{ this_table_name }} AS
  SELECT * FROM `{{ full_staging_table_name }}`
{% endif %}