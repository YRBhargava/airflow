from airflow import DAG
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def run_copy_command(schema, table, s3_bucket, s3_key, redshift_conn_id, aws_access_key_id, aws_secret_access_key):
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    s3_path = f"s3://{s3_bucket}/{s3_key}"
    IAM_ROLE_ARN=os.getenv('REDSHIFT_ROLE_ARN')
    copy_sql = f"""
    COPY {schema}.{table}
    FROM '{s3_path}'
    IAM_ROLE '{IAM_ROLE_ARN}'  
    FORMAT AS JSON 'auto'
    IGNOREHEADER 1;  
    """

    redshift_hook.run(copy_sql)
    move_file_in_s3(s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key)
    
def move_file_in_s3(s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    archive_key = f"archive/users-{timestamp}.json"

    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource={"Bucket": s3_bucket, "Key": s3_key},
        Key=archive_key,
    )

    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id="s3_to_redshift_copy_command",
    default_args=default_args,
    description="Copy data from S3 to Redshift using S3 COPY command",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
) as dag:

    copy_task = PythonOperator(
        task_id="load_data_to_redshift",
        python_callable=run_copy_command,
        op_kwargs={
            "schema": "public",  
            "table": "users",  
            "s3_bucket": os.getenv("S3_DATA_LAKE"),  
            "s3_key": "external/users.json",  
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),  
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),  
            "redshift_conn_id": "redshift_default",  
        },
    )

    copy_task
    