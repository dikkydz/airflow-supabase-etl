import pandas as pd

def etl_netflix_to_raw():
    df = pd.read_csv("/opt/airflow/data/netflix_titles.csv")

    output_path = "/opt/airflow/data/netflix_staging.json"
    df.to_json(output_path, orient="records")

    print("Netflix CSV converted to JSON raw")