from airflow import DAG
import boto3
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import requests
import json

API_URL = 'https://randomuser.me/api/'

def produce_user(**kwargs):
    producer = KafkaProducer(
    bootstrap_servers='kafka:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

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
        producer.send('user_topic', value=user)
        print("Data sent to Kafka topic: user_topic")
    
    producer.close()


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'produce_users',
    default_args=default_args,
    description='Produce user_topic ',
    schedule_interval='@daily',
)

consume_task = PythonOperator(
    task_id='consume_and_insert',
    python_callable=produce_user,
    provide_context=True,
    dag=dag,
)
