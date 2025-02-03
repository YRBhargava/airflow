from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
from datetime import datetime

S3_BUCKET_NAME = 'datalakepax8'
S3_KEY = 'external/users.json'
API_URL = 'https://randomuser.me/api/'

def ingest_user_data():
    random_user_data = fetch_random_user_data()
    save_to_s3(random_user_data)

def fetch_random_user_data():
    data=[]
    for x in range(10):
        response = requests.get(API_URL)
        response.raise_for_status()
        random_user_data = response.json()['results'][0]
        full_name = f"{random_user_data['name']['title']} {random_user_data['name']['first']} {random_user_data['name']['last']}"
        user = {
            "name": full_name,
            "age": random_user_data['dob']['age'],
            "gender": random_user_data['gender'],
            "email": random_user_data['email'],
            "address": f"{random_user_data['location']['street']['number']} "
                       f"{random_user_data['location']['street']['name']}, "
                       f"{random_user_data['location']['city']}, "
                       f"{random_user_data['location']['state']}, "
                       f"{random_user_data['location']['country']} - "
                       f"{random_user_data['location']['postcode']}"
        }
        data.append(user)
    print("------",data)
    return data

def save_to_s3(random_user_data):
    print('SAVE TO S3 CALLED')
    with open("random_users.json", "w") as file:
        for user in random_user_data:
            file.write(json.dumps(user) + "\n")
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')  
    s3_hook.load_file(
        filename="random_users.json",
        key=S3_KEY,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )
    print(f"Data successfully saved to s3://{S3_BUCKET_NAME}/{S3_KEY}")

with DAG(
    dag_id='ingest_random_user_to_s3',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_user_data = PythonOperator(
        task_id='ingest_user_data',
        python_callable=ingest_user_data,
        provide_context=True
    )

    ingest_user_data
