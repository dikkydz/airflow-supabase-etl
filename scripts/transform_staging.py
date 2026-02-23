import json
import pandas as pd
from datetime import datetime

def transform_raw_to_staging(input_file, output_file):
    # Read raw JSON
    with open(f"/opt/airflow/data/{input_file}", "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # ---- SIMPLE CLEANING ----
    df.columns = df.columns.str.lower()

    # drop duplicate
    df = df.drop_duplicates()

    # normalize date column if exists
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # add ingestion timestamp
    df["ingested_at"] = datetime.utcnow()

    # save parquet
    df.to_parquet(f"/opt/airflow/data/{output_file}", index=False)

    print(f"Staging file created: {output_file}")