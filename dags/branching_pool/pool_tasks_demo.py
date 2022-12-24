from datetime import datetime
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import dynamic_process_tasks
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
import time

default_args= {'owner':'jsharma',
    "start_date" : datetime(2022,1,1),
    'email':'my@gmail.com',
    'retries': 1
    }

my_transaction_partners = [
    {
        "transaction_value": 500,
        "priority" : 2 
    },
    {
        "transaction_value": 1000,
        "priority" : 3 
    },
    {
        "transaction_value": 1500,
        "priority" : 4 
    }
]

@dag(default_args=default_args,
    schedule_interval="@Daily",
    description="Pool tasks Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def pool_tasks_demo():
    start =DummyOperator(task_id="start")
    storing = DummyOperator(task_id="storing", trigger_rule='none_failed_or_skipped')

    for transaction in my_transaction_partners:
        transaction_value = transaction['transaction_value']
        priority_val = transaction['priority']
        @task.python(task_id=f"extract_{transaction_value}", priority_weight=priority_val, pool='my_pool',  do_xcom_push=False , multiple_outputs=True)
        def extract(transaction_value):
            return {"transaction_value":transaction_value}
        trx_value = extract(transaction_value)
        start >> trx_value
        dynamic_process_tasks(trx_value) >> storing

pool_tasks_demo()