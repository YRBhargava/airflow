from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime
import os
import logging

SQL_SCRIPTS_DIR = "/opt/airflow/logs/alter"  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def execute_pending_scripts(**kwargs):
    
    postgres_hook = PostgresHook(postgres_conn_id="redshift_default")  

    try:
        executed_scripts = Variable.get("executed_sql_scripts", default_var="[]")
        executed_scripts = eval(executed_scripts)  
    except Exception as e:
        logging.error(f"Failed to fetch executed scripts: {e}")
        executed_scripts = []

    logging.info(f'Executed scripts: {executed_scripts}')

    try:
        all_scripts = sorted([f for f in os.listdir(SQL_SCRIPTS_DIR) if f.endswith(".sql")])
        logging.info(f'All scripts: {all_scripts} in {SQL_SCRIPTS_DIR}')
    except Exception as e:
        logging.error(f"Error reading SQL scripts directory {SQL_SCRIPTS_DIR}: {e}")
        all_scripts = []

    pending_scripts = [script for script in all_scripts if script not in executed_scripts]
    logging.info(f'Pending scripts: {pending_scripts}')

    for script in pending_scripts:
        script_path = os.path.join(SQL_SCRIPTS_DIR, script)
        try:
            with open(script_path, "r") as file:
                sql = file.read()
                postgres_hook.run(sql)
            
            executed_scripts.append(script)
            
            Variable.set("executed_sql_scripts", str(executed_scripts))
            logging.info(f'Successfully executed {script}')
        
        except Exception as e:
            logging.error(f"Failed to execute {script}: {e}")

    logging.info('All pending scripts executed successfully!')

with DAG(
    dag_id="alter",
    default_args=default_args,
    description="Execute pending SQL scripts in sequence",
    schedule_interval="@daily",  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sql", "alter", "scripts"],
) as dag:

    execute_scripts = PythonOperator(
        task_id="execute_pending_scripts",
        python_callable=execute_pending_scripts,
    )
