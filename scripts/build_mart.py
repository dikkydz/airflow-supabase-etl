import pandas as pd

def build_data_mart(api_file, netflix_file, output_file):

    df_api = pd.read_parquet(f"/opt/airflow/data/{api_file}")
    df_netflix = pd.read_parquet(f"/opt/airflow/data/{netflix_file}")

    # Example aggregation
    df_api["source"] = "api"
    df_netflix["source"] = "netflix"

    df_all = pd.concat([df_api, df_netflix], ignore_index=True)

    # Example simple mart: count per source
    mart = df_all.groupby("source").size().reset_index(name="total_records")

    mart.to_parquet(f"/opt/airflow/data/{output_file}", index=False)

    print("Data mart built successfully")