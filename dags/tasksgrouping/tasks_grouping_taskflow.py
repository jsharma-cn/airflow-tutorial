from datetime import datetime
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from subdag.subdag_factory import subdag_factory
from airflow.utils.task_group import TaskGroup
#Subdag operator have sensor behind the scene.

@task.python
def process_1(transaction_value):
    print('Transaction Id : ',transaction_value)

@task.python
def process_2(transaction_value):
    print('Transaction Id : ',transaction_value)

@task.python
def process_3(transaction_value):
    print('Transaction Id : ',transaction_value)



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

def task_groups_taskflow_demo():
    transaction_details = extract()
    with TaskGroup(group_id='process_tasks'
    ) as process_tasks:
        process_1(transaction_details['transaction_value'])
        process_2(transaction_details['transaction_value'])
        process_3(transaction_details['transaction_value'])
    
    transaction_details >> process_tasks

task_groups_taskflow_demo()