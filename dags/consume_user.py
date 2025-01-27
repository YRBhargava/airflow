from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
import psycopg2
import json
import os

load_dotenv()
# Function to consume messages from Kafka and insert into Redshift
def consume_and_insert(**kwargs):
    # Kafka Consumer configuration
    print('----starting consume_and_insert---')
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9093',
        'group.id': 'airflow_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['user_topic'])

    # Redshift connection details
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    cursor = conn.cursor()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Parse Kafka message and insert into Redshift
                record = json.loads(msg.value())
                print('-----Record to be inserted:  ', record)
                insert_query = """
                INSERT INTO users (name, age, gender, email, address) VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (record['name'], record['age'], record['gender'], record['email'], record['address']))
                conn.commit()
    except Exception as error:
        print('exception caught :',error)
    finally:
        consumer.close()
        cursor.close()
        conn.close()

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'consume_users',
    default_args=default_args,
    description='Consume user_topic and insert into Redshift',
    schedule_interval='@daily',
)

consume_task = PythonOperator(
    task_id='consume_and_insert',
    python_callable=consume_and_insert,
    provide_context=True,
    dag=dag,
)
