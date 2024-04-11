from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, date, datetime
import psycopg2
import json
import boto3

# Default ARGS
DEFAULT_ARGS = {
    "start_date": datetime(2023, 1, 1),
    "retries": 2,
    "email_on_retry": False,
    "email_on_failure": False,
}

# Acesso ao Postgres 
PG_HOST="github-copilot.ctdrgq7tvep2.us-east-1.rds.amazonaws.com"
PG_DATABASE="github_copilot"
PG_USER="copilot"
PG_PASSWORD="bL9nzFUQK2pnSCqsCrfc"
PG_TABLE="vendas"

# Bucket e prefix
S3_BUCKET_NAME="github-copilot-desafio"
hoje=date.today()
S3_OBJECT_KEY=f"marcos-mueller/vendas{hoje.strftime('%Y-%m-%d')}"

def upload_to_s3():
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

    cursor = conn.cursor()

    # Executa a consulta SQL
    cursor.execute(f'SELECT * FROM {PG_TABLE}')
    rows = cursor.fetchall()

    # Desconecta do PostgreSQL
    cursor.close()
    conn.close()

    # Converte os dados para formato JSON
    data = []
    for row in rows:
        data.append({
            'id_venda': row[0],
            'data_venda': row[1].strftime('%Y-%m-%d'),
            'quantidade': row[2],
    })

    # Salva os dados no formato JSON
    json_data = json.dumps(data)

    # Conecta-se ao Amazon S3
    s3 = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Envia o arquivo JSON para o S3
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_OBJECT_KEY,
        Body=json_data.encode('utf-8')
    )

    print(f'Dados salvos com sucesso em {S3_BUCKET_NAME}/{S3_OBJECT_KEY}')

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


