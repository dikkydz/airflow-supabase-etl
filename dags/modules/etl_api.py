import requests
import json
from datetime import datetime

def etl_api_to_raw():
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()

    file_path = "/opt/airflow/data/api_staging.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print("API data saved to raw layer")