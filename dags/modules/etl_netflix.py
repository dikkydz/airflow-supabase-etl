import pandas as pd
import os

def etl_netflix_to_raw():
    df = pd.read_csv("/opt/airflow/data/netflix_titles.csv")

    os.makedirs("/opt/airflow/data/raw", exist_ok=True)

    output_path = "/opt/airflow/data/raw/netflix_raw.json"
    df.to_json(output_path, orient="records")

    print("Netflix CSV converted to RAW JSON")