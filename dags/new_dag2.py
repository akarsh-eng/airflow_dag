from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Param
from datetime import datetime
import os
import csv
import requests
import psycopg2

# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------

def _sanitize_identifier(name: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name.strip())
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized.lower() or "col_unnamed"


def ensure_local_dir(org_slug: str) -> str:
    local_dir = os.path.join("/tmp", "airflow_data", org_slug)
    os.makedirs(local_dir, exist_ok=True)
    return local_dir


def _download_via_http_conn(file_url: str, target_path: str) -> bool:
    try:
        resp = requests.get(file_url, timeout=60, stream=True)
        resp.raise_for_status()
        with open(target_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False


def download_csv(file_url: str, local_dir: str, file_name: str) -> str:
    from urllib.request import urlretrieve
    local_path = os.path.join(local_dir, file_name)
    if not _download_via_http_conn(file_url, local_path):
        urlretrieve(file_url, local_path)
    return local_path


def load_csv_to_postgres(local_csv_path: str, schema_name: str, table_name: str) -> int:
    PG_HOST = os.getenv("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com")
    PG_DB = os.getenv("POSTGRES_DATABASE", "postgres")
    PG_USER = os.getenv("SUPABASE_USER", "postgres.gzawhludfyspekqesqgv")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "tECW6ganq2cAh2ik")
    PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )
    conn.autocommit = True
    cur = conn.cursor()

    with open(local_csv_path, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)

    columns = [f'"{_sanitize_identifier(h)}" TEXT' for h in headers]
    qualified_table = f'"{schema_name}"."{_sanitize_identifier(table_name)}"'

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS "{schema_name}";
    CREATE TABLE IF NOT EXISTS {qualified_table} ({', '.join(columns)});
    """
    cur.execute(create_sql)

    cur.execute(f"TRUNCATE TABLE {qualified_table};")

    with open(local_csv_path, "r") as f:
        cur.copy_expert(sql=f"COPY {qualified_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", file=f)

    cur.execute(f"SELECT COUNT(*) FROM {qualified_table};")
    rows_loaded = cur.fetchone()[0] # type: ignore

    cur.close()
    conn.close()
    return rows_loaded


def extract_params(storage_path: str) -> dict:
    path_parts = [p for p in storage_path.split("/") if p]
    file_name = path_parts[-1].split("?")[0]
    org_slug = path_parts[6]
    table_name = os.path.splitext(file_name)[0]

    return {
        "file_url": storage_path,
        "org_slug": org_slug,
        "table_name": table_name,
        "file_name": file_name,
        "pg_schema": org_slug,
    }

# --------------------------------------------------------------------------
# Airflow Task Logic
# --------------------------------------------------------------------------

def download_and_load_csv(**context):
    storage_path = context["params"]["storage_path"]

    # Step 1: Extract params
    rst = extract_params(storage_path)

    # Step 2: Ensure local directory
    local_dir = ensure_local_dir(rst["org_slug"])

    # Step 3: Download
    local_path = download_csv(rst["file_url"], local_dir, rst["file_name"])

    # Step 4: Load into PostgreSQL
    rows_loaded = load_csv_to_postgres(local_path, rst["pg_schema"], rst["table_name"])

    print(f"✅ Loaded {rows_loaded} rows into {rst['pg_schema']}.{rst['table_name']}")
    return rows_loaded

# --------------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------------

with DAG(
    dag_id="load_csv_to_postgres_dag",
    description="Download CSV from Supabase and load into PostgreSQL",
    start_date=datetime(2025, 9, 30),
    schedule=None,  # Trigger manually or via API
    catchup=False,
    tags=["csv", "postgres", "supabase"],
) as dag:

    load_csv_task = PythonOperator(
        task_id="download_and_load_csv",
        python_callable=download_and_load_csv,
        params={
            "storage_path": Param(
                default="https://gzawhludfyspekqesqgv.supabase.co/storage/v1/object/sign/dhruv-new/uploads/2025/09/25/dhruvanand2617_gmail.com/retail_sales_dataset.csv?token=EXAMPLE",
                type="string",
                description="Supabase Storage file URL (signed URL for CSV)",
            ),
        },
    )

    load_csv_task # type: ignore
