from datetime import datetime
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from subdag.subdag_factory import subdag_factory

#Subdag operator have sensor behind the scene.

default_args= {'owner':'jsharma',
    "start_date" : datetime(2022,1,1),
    'email':'my@gmail.com',
    'retries': 1
    }

@task.python(task_id='extract', do_xcom_push=False, multiple_outputs=True)
def extract():
    transaction_value=500
    return {"transaction_value":transaction_value}
    #ti.xcom_push(key="transaction_id", value=transaction_value)

@dag(default_args=default_args,
    schedule_interval="@Daily",
    description="Task Groupings Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def task_groups_subdag_demo():
    process_tasks=SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory('task_groups_subdag_demo',
        "process_tasks",
        default_args)
    )
    extract() >> process_tasks

task_groups_subdag_demo()