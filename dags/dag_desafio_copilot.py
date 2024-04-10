from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta
import psycopg2

# Acesso ao Postgres 
PG_HOST="github-copilot.ctdrgq7tvep2.us-east-1.rds.amazonaws.com"
PG_DATABASE="github_copilot"
PG_USER="copilot"
PG_PASSWORD="bL9nzFUQK2pnSCqsCrfc"


def upload_to_s3():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )



OWNER = {
    "name": "Marcos",
    "email": "marcos.mueller@indicium.tech",
}
with DAG(
    dag_id="dag_el_s3",
    default_args=dict(DEFAULT_ARGS, owner=OWNER["name"]),
    description="Load to s3 json format",
    # schedule="0 5 * * 6",
    max_active_runs=1,
    catchup=False,
    tags=["python", "s3"],
    owner_links={OWNER["name"]: f"mailto:{OWNER['email']}"},
) as dag:
    load_file = PythonOperator(
        task_id="load_file_s3",
        python_callable=upload_to_s3,
        execution_timeout=timedelta(hours=1),
        op_kwargs={
            "bucket_name": "github-copilot-desafio",
            "s3_prefix": f"marcos-mueller/{{{{ data_interval_start }}}}",
        },
        executor_config=DEFAULT_EXECUTOR_CONFIG_OVERRIDE,
    )


