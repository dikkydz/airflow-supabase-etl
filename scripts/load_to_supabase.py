import os
from supabase import create_client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


def upload_to_supabase(file_name: str, layer: str):
    """
    Upload file ke Supabase Storage dengan struktur:
    raw-data/{layer}/{file_name}

    layer: raw | staging | mart
    """

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL atau SUPABASE_KEY belum diset di environment variables")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    local_path = f"/opt/airflow/data/{layer}/{file_name}"
    storage_path = f"{layer}/{file_name}"

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File tidak ditemukan: {local_path}")

    # Tentukan content type otomatis
    if file_name.endswith(".json"):
        content_type = "application/json"
    elif file_name.endswith(".parquet"):
        content_type = "application/octet-stream"
    elif file_name.endswith(".csv"):
        content_type = "text/csv"
    else:
        content_type = "application/octet-stream"

    with open(local_path, "rb") as f:
        supabase.storage.from_("ListData").upload(
            path=storage_path,
            file=f,
            file_options={
                "content-type": content_type,
                "upsert": "true"   
            },
        )

    print(f"✅ {storage_path} uploaded successfully")