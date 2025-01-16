{% macro run_staging_setup() %}
  {% set py_script_path = './python_scripts/csv_load_yml_python.py' %}  -- Updated path

  {% do log("Running staging setup (Python script): " ~ py_script_path, info=True) %}

  {% endmacro %}