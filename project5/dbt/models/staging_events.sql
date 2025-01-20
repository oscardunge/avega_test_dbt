{{ config(materialized='table') }}

{% set staging_table_name = 'staging_csv_data_events_' ~ run_started_at.strftime("%Y%m%d") %}
{% set staging_table = adapter.get_relation(database=target.project, schema=target.dataset, identifier=staging_table_name) %}

SELECT *
FROM {{ staging_table }}



