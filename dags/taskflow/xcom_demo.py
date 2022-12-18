import sqlite3
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Limitations
# Small amount data
# Use references - for backend

default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 1}

def extract(ti):
    transaction_value=500
    return {"transaction_value" : transaction_value}
    #ti.xcom_push(key="transaction_id", value=transaction_value)

def process(ti):
    #transaction_id = ti.xcom_pull(key='transaction_id', task_ids='extract')
    transaction_id = ti.xcom_pull(task_ids='extract')
    print('Transaction Id : ', transaction_id['transaction_value'])


with DAG(dag_id="xcom_demo",
    default_args=default_args,
    start_date=datetime(2022,1,1),
    schedule_interval="@Daily",
    description="Xcom Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    ) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    process_task = PythonOperator(
        task_id='process',
        python_callable=process
    )
    extract_task >> process_task