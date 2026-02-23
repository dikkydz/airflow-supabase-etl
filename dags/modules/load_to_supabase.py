import os
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def upload_to_supabase(file_name):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    file_path = f"/opt/airflow/data/{file_name}"

    try:
        supabase.storage.from_("raw-data").remove([file_name])
        print("Old file removed")
    except Exception:
        print("File not found, skipping delete")

    # 🔥 Upload ulang
    with open(file_path, "rb") as f:
        supabase.storage.from_("raw-data").upload(
            path=file_name,
            file=f,
            file_options={"content-type": "application/json"},
        )

    print(f"{file_name} uploaded successfully")