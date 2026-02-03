# SQL Server to BigQuery ETL

This repository contains a Python ETL pipeline to extract data from Microsoft SQL Server and load it into **Google BigQuery**. It uses a helper class `SQLServer` for connecting, querying, and retrieving results into Pandas DataFrames.

---

## Features

* Connect to SQL Server using `pypyodbc`
* Execute SQL queries and get results with column names
* Load Pandas DataFrames into Google BigQuery
* Authenticate with Google Cloud service account JSON
* Basic error handling and connection checks

---

## Prerequisites

1. **Python 3.8+**
2. Required Python packages:

```bash
pip install pandas pypyodbc google-cloud-bigquery pyarrow
```

Optional:

```bash
pip install fastparquet
```

3. **Google Cloud setup**:

   * BigQuery API enabled
   * Service account JSON key with BigQuery access

4. **SQL Server setup**:

   * Microsoft SQL Server instance
   * Database credentials or Windows authentication

---

## Usage

### Connect to SQL Server

```python
from sqlserver import SQLServer

sql_server_instance = SQLServer('YOUR_SERVER_NAME', 'YOUR_DATABASE_NAME')
sql_server_instance.connect_to_sql_server()
```

### Execute SQL Query

```python
sql_statement = "SELECT * FROM [schema].[table_name]"
columns, records = sql_server_instance.query(sql_statement)

import pandas as pd
df = pd.DataFrame(records, columns=columns)
```

### Connect to Google BigQuery

```python
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r"path\to\service_account.json"
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
```

### Load DataFrame into BigQuery

```python
target_table_id = 'your-project.dataset.table'

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(df, target_table_id, job_config=job_config)
job.result()
print("Data loaded successfully")
```

### Verify Table

```python
table = client.get_table(target_table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {target_table_id}")
```

---

## Notes

* Ensure the dataset exists in BigQuery before running ETL
* Use service account credentials for authentication
* Install `pyarrow` for DataFrame uploads
* `SQLServer` uses Windows authentication by default (`Trust_Connection=yes`). Update `_connection_string()` for SQL login.

---

