# load_staging.py
import os
import yaml
from google.cloud import bigquery
import pandas as pd
from google.cloud.exceptions import GoogleCloudError
import logging
import datetime


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')




def get_dated_staging_table_name(base_table_name, date=None):
    if date is None:
        date = datetime.date.today()
    date_str = date.strftime("%Y%m%d")
    return f"staging_{base_table_name}_{date_str}"

def get_pandas_schema(csv_file_path):
    """Gets the schema from pandas."""
    try:
        df = pd.read_csv(csv_file_path)
        return df.dtypes
    except FileNotFoundError:
        logging.error(f"Error: CSV file not found: {csv_file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.error(f"Error: CSV file is empty: {csv_file_path}")
        return None
    except Exception as e:
        logging.error(f"An error occurred reading csv {csv_file_path} : {e}")
        return None

def create_staging_table_if_not_exists(client, project_id, dataset_id, base_table_name, schema_def, csv_file_path):
    """Checks if a staging table exists and creates it if not."""
    staging_table_name = get_dated_staging_table_name(base_table_name)
    table_ref = f"{project_id}.{dataset_id}.{staging_table_name}"
    try:
        client.get_table(table_ref)  # Try to get the table
        logging.warning(f"Staging table {table_ref} already exists. Skipping creation.")
        return table_ref
    except GoogleCloudError as e:
        if e.code == 404:  # Table not found (expected)
            logging.info(f"Staging table {table_ref} does not exist. Creating table.")
            schema = []
            if schema_def:
                for field in schema_def:
                    schema.append(bigquery.SchemaField(field["name"], field["type"]))
            else:
                pandas_schema = get_pandas_schema(csv_file_path)
                if pandas_schema:
                    for col_name, dtype in pandas_schema.items():
                        schema.append(bigquery.SchemaField(col_name, bigquery.enums.SqlTypeNames.from_python_type(dtype)))
                else:
                    logging.error(f"Could not infer schema for {csv_file_path}. Skipping table creation.")
                    return None
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)  # Create the table
            logging.info(f"Created table {table_ref}.")
            return table_ref
        else:
            raise  # Re-raise other BigQuery errors




def load_to_staging(client, schemas_config, csv_dir):
    """Loads CSV data into BigQuery staging tables with robust error handling."""
    files_loaded = 0
    project_id = schemas_config.get("project_id")
    dataset_id = schemas_config.get("dataset")
    if not project_id or not dataset_id:
        raise ValueError("Project and dataset must be defined in schemas.yml")

    error_logs = []

    for table_name in schemas_config.get("schemas", {}):
        csv_file_path = os.path.join(csv_dir, f"{table_name}.csv")


        table_def = schemas_config.get("schemas", {}).get(table_name)

        table_ref = create_staging_table_if_not_exists(client, project_id, dataset_id, table_name, table_def)
        if table_ref is None:
            continue

        try:
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.strip()


            if table_name:
                schema = [bigquery.SchemaField(field["name"], field["type"]) for field in table_name]
                for field in table_name:
                    if field["name"] in df.columns:
                        if field["type"] == "DATE":
                            try:
                                df[field["name"]] = pd.to_datetime(df[field["name"]], format="%Y-%m-%d", errors='coerce').dt.strftime('%Y-%m-%d')
                            except (pd.errors.ParserError, ValueError):
                                logging.warning(f"Failed to parse date column '{field['name']}' in {table_name}.csv. Using object type.")
                        elif field["type"] == "DATETIME":
                            try:
                                df[field["name"]] = pd.to_datetime(df[field["name"]], format="%Y-%m-%d %H:%M:%S", errors='coerce')
                            except (pd.errors.ParserError, ValueError):
                                try:
                                    df[field["name"]] = pd.to_datetime(df[field["name"]], format="%Y-%m-%d", errors='coerce')
                                except (pd.errors.ParserError, ValueError):
                                    logging.warning(f"Failed to parse datetime column '{field['name']}' in {table_name}.csv. Using object type.")

            else:
                schema = [bigquery.SchemaField(col, bigquery.enums.SqlTypeNames.from_python_type(df[col].dtype)) for col in df.columns]


            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
            )
            destination_table = client.get_table(table_ref)
            # logging.info(f"Loading data to BigQuery table: {destination_table.full_table_id}")
            load_job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
            load_job.result()
            files_loaded += 1
            logging.info(f"Loaded table: {table_ref}")

        except GoogleCloudError as e:
            error_logs.append({
                "table_name": table_name,
                "file_path": csv_file_path,
                "error_message": str(e),
                "error_details": e.errors if hasattr(e, "errors") else None
            })
            logging.error(f"BigQuery error loading {csv_file_path}: {e}")
        except FileNotFoundError as e:
            error_logs.append({
                "table_name": table_name,
                "file_path": csv_file_path,
                "error_message": str(e),
                "error_details": None
            })
            logging.error(f"File not found: {csv_file_path}: {e}")
        except Exception as e:
            error_logs.append({
                "table_name": table_name,
                "file_path": csv_file_path,
                "error_message": str(e),
                "error_details": None
            })
            logging.error(f"General error loading {csv_file_path}: {e}")

    if error_logs:  # Print error summary after all loading attempts
        print(f"\n{files_loaded} files loaded successfully!")
        error_df = pd.DataFrame(error_logs)
        print("\n--- Summary of Errors ---")

        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.width', 500)

        print(error_df.T)

        pd.reset_option('display.max_colwidth')
        pd.reset_option('display.width')

    else:
        print(f"\nALL {files_loaded} files loaded successfully!")




def main():
    with open("config.yml", "r") as f:
        schemas_config = yaml.safe_load(f)
    project_root = schemas_config.get("project_root")
    credentials_relative_path = schemas_config.get("credentials_path")

    
    if project_root and credentials_relative_path:
        credentials_path = os.path.join(project_root, credentials_relative_path) # Construct full path
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(credentials_path)
        print(f"Using credentials from: {credentials_path}")
    elif credentials_relative_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(credentials_relative_path)
        print(f"Using credentials from: {credentials_relative_path}")
    else:
        print("No credentials_path found in schemas.yml. Using Application Default Credentials.")

    client = bigquery.Client()

    csv_dir = os.path.join(project_root, "dbt", "csv_data")
    print(f"CSV Directory: {csv_dir}")

    load_to_staging(client, schemas_config, csv_dir )






if __name__ == "__main__":
    main()











