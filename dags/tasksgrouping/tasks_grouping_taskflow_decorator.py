from datetime import datetime
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks

@task.python(task_id='extract', do_xcom_push=False, multiple_outputs=True)
def extract():
    transaction_value=500
    return {"transaction_value":transaction_value}

default_args= {'owner':'jsharma',
    "start_date" : datetime(2022,1,1),
    'email':'my@gmail.com',
    'retries': 1
    }

@dag(default_args=default_args,
    schedule_interval="@Daily",
    description="Task Groupings Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def task_groups_taskflow_decorator_demo():
    transaction_details = extract()
    process_task = process_tasks(transaction_details)
    transaction_details >> process_task

task_groups_taskflow_decorator_demo()