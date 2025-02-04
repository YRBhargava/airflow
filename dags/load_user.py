from airflow import DAG
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def list_s3_keys(s3_bucket, prefix, aws_access_key_id, aws_secret_access_key):
    """
    List all JSON keys in the specified S3 prefix
    """
    print('Running list_s3_keys')
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    paginator = s3_client.get_paginator('list_objects_v2')
    keys = []
    
    for result in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        if 'Contents' in result:
            keys.extend([
                obj['Key'] for obj in result['Contents'] 
                if obj['Key'].endswith('.json')
            ])
    print('Finished list_s3_keys')
    return keys

def run_copy_command(s3_bucket, s3_key, redshift_conn_id, aws_access_key_id, aws_secret_access_key):
    """
    Copy a single S3 file to Redshift and archive it
    """
    print('Running run_copy_command')
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    s3_path = f"s3://{s3_bucket}/{s3_key}"

    IAM_ROLE_ARN = os.getenv('REDSHIFT_ROLE_ARN')
    
    path_parts = s3_key.split('/')
    table = 'users'
    schema = 'public'  
    
    copy_sql = f"""
    COPY {schema}.{table}
    FROM '{s3_path}'
    IAM_ROLE '{IAM_ROLE_ARN}'  
    TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS'
    FORMAT AS JSON 'auto'
    IGNOREHEADER 1;  
    """

    redshift_hook.run(copy_sql)
    
    move_file_in_s3(s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key)
    print('Finished run_copy_command')

def move_file_in_s3(s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    """
    Move processed file to archive folder
    """
    print('Running move_files_in_s3')
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    archive_key = f'archived/users/{timestamp}'

    s3_client.copy_object(
        Bucket=s3_bucket,
        CopySource={"Bucket": s3_bucket, "Key": s3_key},
        Key=archive_key,
    )

    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    print('Finished move_files_in_s3')

def process_s3_files(**context):
    """
    Main task to list and process S3 files
    """
    print('Running processs3 files')
    s3_bucket = os.getenv("S3_DATA_LAKE")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    redshift_conn_id = "redshift_default"
    
    s3_keys = list_s3_keys(
        s3_bucket, 
        prefix='external/', 
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key
    )
    
    for s3_key in s3_keys:
        run_copy_command(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            redshift_conn_id=redshift_conn_id,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    print('Finished process_s3_files')

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id="s3_to_redshift_copy_command",
    default_args=default_args,
    description="Dynamically copy multiple files from S3 to Redshift",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
) as dag:

    process_files_task = PythonOperator(
        task_id="process_s3_files",
        python_callable=process_s3_files,
        provide_context=True,
    )

    process_files_task