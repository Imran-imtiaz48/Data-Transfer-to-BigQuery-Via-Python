import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ----------------------
# BigQuery setup
# ----------------------
KEY_PATH = r"path.json"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

# ----------------------
# Read CSV from local machine
# ----------------------
CSV_PATH = r"path.csv"

try:
    df = pd.read_csv(CSV_PATH)
    print(f"CSV loaded successfully. Shape: {df.shape}")
except Exception as e:
    print(f"Failed to read CSV: {e}")
    exit(1)

# ----------------------
# Load to BigQuery
# ----------------------
DATASET_ID = 'etl_python1'      # Your BigQuery dataset
TABLE_ID = 'dataFromLocalMachine'  # Target table name
TARGET_TABLE = f"{client.project}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    autodetect=True,           # Let BigQuery detect schema
    write_disposition='WRITE_TRUNCATE'  # Overwrite existing table
)

try:
    job = client.load_table_from_dataframe(df, TARGET_TABLE, job_config=job_config)
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
    print("Data loaded successfully to BigQuery.")
except Exception as e:
    print(f"Failed to load data to BigQuery: {e}")
    exit(1)

# ----------------------
# Verify loaded table
# ----------------------
try:
    table = client.get_table(TARGET_TABLE)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to '{TARGET_TABLE}'"
    )
except Exception as e:
    print(f"Failed to fetch table info: {e}")
