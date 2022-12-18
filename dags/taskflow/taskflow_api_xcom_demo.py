from datetime import datetime
from airflow.decorators import task, dag
from typing import Dict
# Task flow
# Decorators - easier way to create Dag
# Xcom args - setup xcoms

default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 1}

#@task.python(task_id='extract_values', multiple_outputs=True)
@task.python(task_id='extract_values', do_xcom_push=False, multiple_outputs=True)
def extract():
#@task.python(task_id='extract')
#def extract()-> Dict[str,str]:
    transaction_value=500
    return {'transaction_value':transaction_value, 'name':'Sam'}

@task.python
def process(transaction_value, name):
    print('Transaction value : ',transaction_value)
    print('Name : ', name)


@dag(default_args=default_args,
    start_date=datetime(2022,1,1),
    schedule_interval="@Daily",
    description="Xcom Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    )

def task_flow_api_xcom_demo():
    my_dict = extract()
    process(my_dict['transaction_value'], my_dict['name'])

task_flow_api_xcom_demo()