from datetime import datetime
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import dynamic_process_tasks
from airflow.operators.dummy import DummyOperator

#Dynamic tasks does not work on output of any tasks
# Dynamic tasks work on predefined dict or list

default_args= {'owner':'jsharma',
    "start_date" : datetime(2022,1,1),
    'email':'my@gmail.com',
    'retries': 1
    }

my_transaction_partners = [
    {
        "transaction_value": 500
    },
    {
        "transaction_value": 1000
    },
    {
        "transaction_value": 1500
    }
]

@dag(default_args=default_args,
    schedule_interval="@Daily",
    description="Task Groupings Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def dynamic_tasks_demo():
    start =DummyOperator(task_id="start")
    for transaction in my_transaction_partners:
        transaction_value = transaction['transaction_value']
        @task.python(task_id=f"extract_{transaction_value}", do_xcom_push=False, multiple_outputs=True)
        def extract(transaction_value):
            return {"transaction_value":transaction_value}
        trx_value = extract(transaction_value)
        start >> trx_value
        dynamic_process_tasks(trx_value)

dynamic_tasks_demo()