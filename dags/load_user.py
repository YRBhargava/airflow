from airflow import DAG
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to run the COPY command
def run_copy_command(schema, table, s3_bucket, s3_key, redshift_conn_id, aws_access_key_id, aws_secret_access_key):
    # Redshift connection hook
    redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

    # Construct the S3 path (ensure the file format and path are correct)
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    # Construct the COPY SQL query
    copy_sql = f"""
    COPY {schema}.{table}
    FROM '{s3_path}'
    IAM_ROLE 'arn:aws:iam::654654432597:role/s3-access-to-redshift'  
    FORMAT AS JSON 'auto'
    IGNOREHEADER 1;  
    """

    # Run the COPY command
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
    

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id="s3_to_redshift_copy_command",
    default_args=default_args,
    description="Copy data from S3 to Redshift using S3 COPY command",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Set your desired schedule
) as dag:

    # Task to call the function
    copy_task = PythonOperator(
        task_id="load_data_to_redshift",
        python_callable=run_copy_command,
        op_kwargs={
            "schema": "public",  # Your Redshift schema
            "table": "users",  # Your Redshift table name
            "s3_bucket": "datalakepax8",  # Your S3 bucket name
            "s3_key": "external/users.json",  # Path to the file in S3
            "aws_access_key_id": "AKIAZQ3DR4VKSDXA6OEJ",  # Your AWS Access Key
            "aws_secret_access_key": "rQI9COcUQdvA0mqE5HMoQ337TpmkLGe3CLzuNRd1",  # Your AWS Secret Key
            "redshift_conn_id": "redshift_default",  # Your Airflow Redshift connection ID
        },
    )

    copy_task
    