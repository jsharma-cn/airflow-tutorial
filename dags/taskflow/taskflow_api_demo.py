import sqlite3
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
# Task flow
# Decorators - easier way to create Dag
# Xcom args - setup xcoms

default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 1}

@task.python
def extract():
    transaction_value=500
    return transaction_value
    #ti.xcom_push(key="transaction_id", value=transaction_value)
@task.python
def process(transaction_value):
    print('Transaction Id : ',transaction_value)


@dag(default_args=default_args,
    start_date=datetime(2022,1,1),
    schedule_interval="@Daily",
    description="Xcom Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def task_flow_demo():
    process(extract())

task_flow_demo()