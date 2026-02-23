import requests
import json
import os

def etl_api_to_raw():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()

    os.makedirs("/opt/airflow/data/raw", exist_ok=True)

    file_path = "/opt/airflow/data/raw/api_raw.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print("API data saved to RAW layer")