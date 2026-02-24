import json
import pandas as pd
import os
from datetime import datetime as dt

def transform_raw_to_staging(input_file, output_file):
    """Transform RAW JSON -> STAGING Parquet"""
    os.makedirs("/opt/airflow/data/staging", exist_ok=True)
    raw_path = f"/opt/airflow/data/raw/{input_file}"

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"{input_file} not found in raw folder")

    with open(raw_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["ingested_at"] = dt.utcnow().isoformat()
    df.to_parquet(f"/opt/airflow/data/staging/{output_file}", index=False, engine="pyarrow")

    print(f"STAGING file created: {output_file}")


def build_data_mart(api_file, netflix_file, output_file):
    """Build simple MART layer"""
    os.makedirs("/opt/airflow/data/mart", exist_ok=True)

    df_api = pd.read_parquet(f"/opt/airflow/data/staging/{api_file}", engine="pyarrow")
    df_netflix = pd.read_parquet(f"/opt/airflow/data/staging/{netflix_file}", engine="pyarrow")

    df_api["source"] = "api"
    df_netflix["source"] = "netflix"

    df_all = pd.concat([df_api, df_netflix], ignore_index=True)
    mart = df_all.groupby("source").size().reset_index(name="total_records")
    mart["generated_at"] = dt.utcnow().isoformat()

    df_mart_path = f"/opt/airflow/data/mart/{output_file}"
    mart.to_parquet(df_mart_path, index=False, engine="pyarrow")

    print(f"MART file created: {df_mart_path}")