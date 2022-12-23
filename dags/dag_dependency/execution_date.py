from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args ={
    "owner":"jsharma",
    "retries":0
}

def _my_func():
    context = get_current_context()
    print('My function is called', context['execution_date'])

with DAG(dag_id="MyParentDag", 
    start_date=datetime(2022,12,15),
    catchup=True,
    schedule_interval="@Daily",
    default_args=default_args,
    tags=["DE"]
    ) as dag:
    
    myOp = PythonOperator(task_id='my_func',
        python_callable=_my_func
    )

    stop= DummyOperator(task_id = 'stop')

myOp >> stop



    