from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args= {'Owner':'jsharma',
    'email':'my@gmail.com'}

with DAG(dag_id="hello_world",
    description="My Frist hello world",
    default_args= default_args,
    start_date=datetime(2022,1,1), 
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), 
    tags=["DE"], 
    catchup=False,
    max_active_runs=1) as dag:
    
    bash_task = BashOperator(
    task_id="bash_task",
    bash_command="echo 'Welcome to Aiflow!!'"
    )

    dummy_task = DummyOperator(task_id="dummy_task")
    dummy_task >> bash_task