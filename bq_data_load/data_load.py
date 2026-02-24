import os
import pandas as pd

from playlist_revision_schema import PLAYLIST_REVISION_SCHEMA
from google.cloud import bigquery

service_account_path = "service_account.json"
project_id = os.environ.get("DBT_PROJECT")
dataset_id = os.environ.get("DBT_DATASET")
table_id = "playlist_summary_external"
file_path = "playlist_summary_external.txt"

client = bigquery.Client.from_service_account_json(service_account_path, project=project_id)

# Metadata fields added for tracking
df = pd.read_csv(file_path, sep="\t", index_col=0)


job_config = bigquery.LoadJobConfig(
    # schema=PLAYLIST_REVISION_SCHEMA
)

with open(file_path, "rb") as source_file:
    load_job = client.load_table_from_dataframe(df, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config)

load_job.result()
