# 📦 Multi Source ETL Pipeline
Data Engineer – Take Home Test

## 📌 Project Overview

Project ini merupakan implementasi pipeline ETL sederhana yang:

Mengambil data dari:

- Public Open API
- Dummy dataset (Netflix dataset)
- Mengirimkan data ke Cloud Storage gratis (Supabase Free Tier)
- Menggunakan Apache Airflow sebagai orchestrator

Menerapkan arsitektur data berlapis:
 - Raw
 - Staging
 - Mart

Pipeline ini dapat dijalankan ulang (reproducible) menggunakan Docker.

# 🏗️ Architecture

                ┌──────────────┐
                │  Public API  │
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │   Extract     │
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │     RAW       │  (JSON)
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │   STAGING     │  (Parquet - Cleaned)
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │     MART      │  (Aggregated Summary)
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │   Supabase    │
                └──────────────┘

# 🧱 Tech Stack

| Component  | Technology |
| ------------- | ------------- |
| Orchestration  | Apache Airflow (Dockerized)  |
| Storage  | Supabase Storage (Free Tier)  |
| Language | Python 3.12  |
| Format   | JSON & Parquet  |
| Containerization | Docker & Docker Compose  |

# 📂 Project Structure

```
airflow-supabase-etl/
│
├── dags/
│   ├── multi_source_etl.py
│   └── modules/
│       ├── etl_api.py
│       ├── etl_netflix.py
│       ├── transform_staging.py
│       ├── transform_mart.py
│       └── load_to_supabase.py
│
├── data/
│   ├── raw/
│   ├── staging/
│   └── mart/
│
├── docker-compose.yml
├── requirements.txt
├── credentials.json
└── README.md
```

# 📊 Data Layer Explanation

1️⃣ RAW Layer

- Format: JSON
- Berisi data mentah hasil scraping
- Tidak ada transformasi
Contoh file:
- api_raw.json
- netflix_raw.json

Tujuan:
Menjaga reproducibility dan auditability data.

2️⃣ STAGING Layer

- Format: Parquet
Transformasi sederhana dilakukan:

- Cleaning null values
- Normalisasi nama kolom
- Standarisasi format tanggal

Tujuan:
Menyiapkan data untuk analytics.

3️⃣ MART Layer

- Format: Parquet
- Berisi data agregasi
Contoh output:
```
{"source":"api","total_records":100,"generated_at":"2026-02-24T06:30:00Z"}
{"source":"netflix","total_records":8807,"generated_at":"2026-02-24T06:30:00Z"}
```

Tujuan:
Menyediakan dataset siap konsumsi untuk reporting atau BI tools.

# 🚀 How To Run
```
1️⃣ Clone Repository
git clone <your-repository-url>
cd airflow-supabase-etl
```

2️⃣ Setup Environment Variables

Buat file .env:
```
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=your_supabase_key 


# Masukkan Fernet Key hasil generate di sini
# docker run --rm apache/airflow:2.9.0 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=


# Masukkan Secret Key hasil generate di sini
# docker run --rm apache/airflow:2.9.0 python -c "import secrets; print(secrets.token_hex(32))"
AIRFLOW__WEBSERVER__SECRET_KEY=

```
3️⃣ Build & Run Docker
```
docker compose up -d
```
4️⃣ Initialize Airflow Database (First Time Only)
```
docker exec -it airflow-webserver airflow db init
```
5️⃣ Access Airflow UI

Buka browser:
```
http://localhost:8080
```
Default login:
```
docker exec -it airflow-webserver bash

airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@email.com \
  --password admin
```

6️⃣ Trigger DAG

1. Aktifkan DAG multi_source_etl
2. Klik Trigger DAG
 
Pipeline akan berjalan:
```
extract_api
extract_netflix
        ↓
upload_raw
        ↓
transform_staging
        ↓
transform_mart
        ↓
upload_to_supabase
```

# ☁️ Supabase Storage

Bucket: 
``` 
ListData 
```

Files generated:

- api_raw.json
- netflix_raw.json
- api_staging.parquet
- netflix_staging.parquet
- mart_summary.parquet

# 🧠 Design Decisions
Kenapa Supabase?

- Free tier
- Mudah diintegrasikan
- Tidak perlu setup cloud kompleks
- REST-based API sederhana

Kenapa Parquet?

- Columnar format
- Lebih efisien untuk analytics
- Compressed & performant
- Industry standard untuk data engineering

Kenapa Airflow?

- Industry standard orchestrator
- Mendukung scheduling
- Modular & scalable
- Mudah di-scale ke production

⭐ Bonus Implemented

✔ Transformasi data sederhana <br/>
✔ Arsitektur raw → staging → mart <br/>
✔ Orkestrasi dengan Airflow <br/>
✔ Cloud storage integration <br/> 
✔ Dockerized environment <br/>