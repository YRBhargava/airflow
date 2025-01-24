from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Define the callable function
def load_s3_to_redshift(schema, table, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key, redshift_conn_id):
    # Initialize the S3 Hook with the credentials
    s3_hook = S3Hook(aws_conn_id=None, client_type="s3")
    s3_hook.client = s3_hook.get_client_type("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Check if the file exists in the S3 bucket
    if s3_hook.check_for_key(s3_key, s3_bucket):
        print(f"File {s3_key} found in S3 bucket {s3_bucket}. Proceeding with copy to Redshift.")
        
        # Initialize the Postgres Hook for Redshift connection
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        # Construct COPY command
        copy_query = f"""
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE '{redshift_hook.get_connection(redshift_conn_id).extra_dejson["iam_role"]}'
            CSV
            IGNOREHEADER 1
            DELIMITER ','
            REGION 'us-west-2';
        """
        
        # Run the COPY command to load data into Redshift
        redshift_hook.run(copy_query)
        print(f"Data copied from S3 {s3_key} to Redshift {schema}.{table}")
    else:
        print(f"File {s3_key} not found in S3 bucket {s3_bucket}.")


# Default Arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

# DAG definition
with DAG(
    dag_id="copy_s3_to_redshift_callable",
    default_args=default_args,
    description="Copy data from S3 to Redshift using a callable function",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Set your desired schedule
) as dag:

    # Task to call the function
    copy_task = PythonOperator(
        task_id="load_data_to_redshift",
        python_callable=load_s3_to_redshift,
        op_kwargs={
            "schema": "dev",  # Your Redshift schema
            "table": "user_details",  # Your Redshift table name
            "s3_bucket": "datalakepax8",  # Your S3 bucket name
            "s3_key": "external/user_data.json",  # Path to the file in S3
            "aws_access_key_id": "AKIAZQ3DR4VKT32RUHEZ",  # Your AWS Access Key
            "aws_secret_access_key": "your-secret-access-key",  # Your AWS Secret Key
            "redshift_conn_id": "redshift_default",  # Your Airflow Redshift connection ID
        },
    )

    copy_task
