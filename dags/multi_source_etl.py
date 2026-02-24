# dags/multi_source_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# import modul dari scripts
from scripts.etl_api import etl_api_to_raw
from scripts.etl_netflix import etl_netflix_to_raw
from scripts.load_to_supabase import upload_to_supabase
from scripts.transform import transform_raw_to_staging, build_data_mart  # buat file transform.py baru

# ==============================
# DAG
# ==============================

with DAG(
    dag_id="multi_source_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        "owner": "dikky",
        "retries": 1,
    }
) as dag:

    # ======================
    # EXTRACT → RAW
    # ======================

    # pastikan folder raw/staging/mart ada
    os.makedirs("/opt/airflow/data/raw", exist_ok=True)
    os.makedirs("/opt/airflow/data/staging", exist_ok=True)
    os.makedirs("/opt/airflow/data/mart", exist_ok=True)

    api_task = PythonOperator(
        task_id="extract_api",
        python_callable=etl_api_to_raw
    )

    netflix_task = PythonOperator(
        task_id="extract_netflix",
        python_callable=etl_netflix_to_raw
    )

    # ======================
    # UPLOAD RAW
    # ======================

    upload_api_raw = PythonOperator(
        task_id="upload_api_raw",
        python_callable=lambda: upload_to_supabase("api_raw.json", "raw")
    )

    upload_netflix_raw = PythonOperator(
        task_id="upload_netflix_raw",
        python_callable=lambda: upload_to_supabase("netflix_raw.json", "raw")
    )

    # ======================
    # TRANSFORM → STAGING
    # ======================

    transform_api = PythonOperator(
        task_id="transform_api_staging",
        python_callable=lambda: transform_raw_to_staging(
            "api_raw.json",
            "api_staging.parquet"
        )
    )

    transform_netflix = PythonOperator(
        task_id="transform_netflix_staging",
        python_callable=lambda: transform_raw_to_staging(
            "netflix_raw.json",
            "netflix_staging.parquet"
        )
    )

    # ======================
    # UPLOAD STAGING
    # ======================

    upload_api_staging = PythonOperator(
        task_id="upload_api_staging",
        python_callable=lambda: upload_to_supabase("api_staging.parquet", "staging")
    )

    upload_netflix_staging = PythonOperator(
        task_id="upload_netflix_staging",
        python_callable=lambda: upload_to_supabase("netflix_staging.parquet", "staging")
    )

    # ======================
    # BUILD MART
    # ======================

    build_mart_task = PythonOperator(
        task_id="build_data_mart",
        python_callable=lambda: build_data_mart(
            "api_staging.parquet",
            "netflix_staging.parquet",
            "mart_summary.parquet"
        )
    )

    # ======================
    # UPLOAD MART
    # ======================

    upload_mart = PythonOperator(
        task_id="upload_mart",
        python_callable=lambda: upload_to_supabase("mart_summary.parquet", "mart")
    )

    # ======================
    # DEPENDENCIES
    # ======================

    api_task >> upload_api_raw >> transform_api >> upload_api_staging
    netflix_task >> upload_netflix_raw >> transform_netflix >> upload_netflix_staging

    [upload_api_staging, upload_netflix_staging] >> build_mart_task >> upload_mart