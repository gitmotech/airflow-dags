from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import requests
import json

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG
dag = DAG(
    'data_pipeline_example',
    default_args=default_args,
    description='A simple data pipeline DAG',
    schedule_interval=timedelta(days=1),  # Run daily
)

# Define functions to be used in tasks
def fetch_data():
    """Task to fetch data from a REST API"""
    url = 'https://jsonplaceholder.typicode.com/posts/1'
    response = requests.get(url)
    return response.json()

def process_data(**kwargs):
    """Task to process data"""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_task')
    processed_data = {
        'processed_data': data['title'].upper()
    }
    return processed_data

# Define tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

store_data_task = BashOperator(
    task_id='store_data_task',
    bash_command='echo "{{ ti.xcom_pull(task_ids=\'process_data_task\') }}" > /tmp/processed_data.txt',
    dag=dag,
)

# Define task dependencies
fetch_data_task >> process_data_task >> store_data_task

# Optionally, you can define more tasks and dependencies as per your pipeline requirements
