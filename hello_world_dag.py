from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 16),
    'retries': 1
}

# Instantiate a DAG object
dag = DAG(
    'hello_world',
    default_args=default_args,
    schedule_interval=None  # This DAG is not scheduled, trigger manually
)

# Define tasks
task_hello = BashOperator(
    task_id='hello_task',
    bash_command='echo "Hello, World!"',
    dag=dag
)

# Define task dependencies (if any)
task_hello  # This DAG has only one task

