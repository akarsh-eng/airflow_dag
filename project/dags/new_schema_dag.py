from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from airflow.exceptions import AirflowFailException # type: ignore
# from airflow.models import Variable # type: ignore

import os
import psycopg2
import pandas as pd
import io
import requests
from dotenv import load_dotenv

# --------------------------------------------------------------------------
# Load environment variables and initialize Supabase client
# --------------------------------------------------------------------------
load_dotenv()


# DAG input parameters (can be set via Airflow Variables or hardcoded)
# DAG_INPUT = {
#     "schema": Variable.get("supabase_schema", default_var="your_schema"),
#     "table_name": Variable.get("supabase_table_name", default_var="your_table")
# }
# DAG_INPUT = {
#     "schema": "dhruv-new",
#     "table_name":"retail_sales_dataset"
# }


# Supabase environment variables (set these in Airflow or your env)
SUPABASE_URL = os.getenv("SUPABASE_URL","")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY","")
# SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

# Postgres connection parameters (set these in Airflow Variables or env)
PG_HOST = os.getenv("SUPABASE_HOST")
PG_DB = os.getenv("POSTGRES_DATABASE")
PG_USER = os.getenv("SUPABASE_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_PORT = int(os.getenv("POSTGRES_PORT"))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 3),
    'depends_on_past': False,
    'retries': 0
}

with DAG(
    dag_id="supabase_private_etl_streaming",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["supabase", "private", "etl", "streaming"],
    params={
        "schema": "dhruv-new",
        "table_name": "retail_sales_dataset",
    },
) as dag:

    def fetch_metadata(**context):
        # Prefer values passed when triggering the DAG (dag_run.conf via UI/CLI),
        # then fall back to DAG params, then environment variables.
        dag_run = context.get("dag_run")
        run_conf = getattr(dag_run, "conf", {}) if dag_run else {}
        params = context.get("params", {})
        schema = run_conf.get("schema") or params.get("schema") or os.getenv("SUPABASE_SCHEMA", "dhruv-new")
        table_name = run_conf.get("table_name") or params.get("table_name") or os.getenv("SUPABASE_TABLE", "retail_sales_dataset")

        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
        )
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f"""
                SELECT table_name, metadata_table_name, storage_path
                FROM "{schema}".dataset_master
                WHERE table_name = %s
            """, (table_name,))
            # cur.execute(f"""
            #     SELECT table_name, metadata_table_name
            #     FROM "{schema}".dataset_master
            #     WHERE table_name = %s
            # """, (table_name,))
            row = cur.fetchone()
            if not row:
                # raise
                raise AirflowFailException(f"No metadata found for table: {table_name}")

            _, metadata_table_name, storage_path = row
            print("Row Fetched:", row)
            # _, metadata_table_name = row
            context['ti'].xcom_push(key='metadata_table', value=metadata_table_name)
            context['ti'].xcom_push(key='storage_path', value=storage_path)
            # context['ti'].xcom_push(key='storage_path', value="uploads/2025/09/25/dhruvanand2617_gmail.com/retail_sales_dataset.csv")

        finally:
            print(context["ti"])
            cur.close()
            conn.close()

    def drop_disabled_columns(**context):
        dag_run = context.get("dag_run")
        run_conf = getattr(dag_run, "conf", {}) if dag_run else {}
        params = context.get("params", {})
        schema = run_conf.get("schema") or params.get("schema") or os.getenv("SUPABASE_SCHEMA", "dhruv-new")
        table_name = run_conf.get("table_name") or params.get("table_name") or os.getenv("SUPABASE_TABLE", "retail_sales_dataset")
        metadata_table = context['ti'].xcom_pull(key='metadata_table', task_ids='fetch_metadata')
        print("Metadata Table:", metadata_table)

        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
        )
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f"""
                SELECT field FROM "{schema}"."{metadata_table}"
                WHERE enabled = FALSE
            """)
            disabled_cols = [row[0] for row in cur.fetchall()]
            context['ti'].xcom_push(key='disabled_cols', value=disabled_cols)
            if not disabled_cols:
                print("No disabled columns to drop.")
                return

            drop_clauses = ", ".join([f'DROP COLUMN IF EXISTS "{col}"' for col in disabled_cols])
            alter_sql = f'ALTER TABLE "{schema}"."{table_name}" {drop_clauses};'
            cur.execute(alter_sql)
            print(f"Dropped columns: {disabled_cols}")
        finally:
            cur.close()
            conn.close()

    def load_data_stream(**context):
        dag_run = context.get("dag_run")
        run_conf = getattr(dag_run, "conf", {}) if dag_run else {}
        params = context.get("params", {})
        schema = run_conf.get("schema") or params.get("schema") or os.getenv("SUPABASE_SCHEMA", "dhruv-new")
        table_name = run_conf.get("table_name") or params.get("table_name") or os.getenv("SUPABASE_TABLE", "retail_sales_dataset")
        storage_path = context['ti'].xcom_pull(key='storage_path', task_ids='fetch_metadata')
        disabled_cols = context['ti'].xcom_pull(key='disabled_cols', task_ids='drop_disabled_columns')
        print(type(disabled_cols), disabled_cols, "Disabled Columns")
        disabled_cols[0] = disabled_cols[0].capitalize()

        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
        )
        conn.autocommit = True
        cur = conn.cursor()

        url = f"{SUPABASE_URL}/storage/v1/object/{schema}/{storage_path}"
        headers = {"Authorization": f"Bearer {SUPABASE_KEY}"}

        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        try:
            # Read CSV in chunks and COPY into Postgres
            for chunk in pd.read_csv(response.raw, chunksize=1000):
                # Remove 'gender' column if present
                if disabled_cols[0] in chunk.columns:
                    chunk = chunk.drop(columns=disabled_cols)
                csv_buffer = io.StringIO()
                chunk.to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)

                full_table = f'"{schema}"."{table_name}"'

                cur.execute(f"TRUNCATE TABLE {full_table};")

                print(f"{full_table} truncated before loading new data.")

                sql = f"COPY {full_table} FROM STDIN WITH CSV"
                cur.copy_expert(sql, csv_buffer)

                print(f"Inserted {len(chunk)} rows into {full_table}")
        finally:
            cur.close()
            conn.close()

    fetch_metadata_task = PythonOperator(
        task_id="fetch_metadata",
        python_callable=fetch_metadata,
    )

    drop_columns_task = PythonOperator(
        task_id="drop_disabled_columns",
        python_callable=drop_disabled_columns
    )

    load_data_task = PythonOperator(
        task_id="load_data_stream",
        python_callable=load_data_stream
    )

    fetch_metadata_task >> drop_columns_task >> load_data_task
# class DummyTI:
#     def __init__(self):
#         self.xcom_store = {}
#     def xcom_push(self, key, value):
#         print(f"Mock xcom_push: {key} = {value}")
#         self.xcom_store[key] = value
#     def xcom_pull(self, key):
#         value = self.xcom_store.get(key)
#         print(f"Mock xcom_pull: {key} -> {value}")
#         return value


# def main():
#     context = {'ti': DummyTI()}
#     fetch_metadata(**context) #>> drop_columns_task >> load_data_task

#     drop_disabled_columns(**context)

#     load_data_stream(**context)

# if __name__ == "__main__":
#     main()


