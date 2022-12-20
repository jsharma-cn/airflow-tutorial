from ast import With
from airflow.decorators import task
from datetime import datetime
from airflow.utils.task_group import TaskGroup

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

@task.python
def child_1():
    print('Child one')

@task.python
def child_2():
    print('Child two')

@task.python
def child_3():
    print('Child three')

def process_tasks(transaction_details):
    with TaskGroup(group_id="process_tasks") as process_tasks:
        with TaskGroup(group_id = 'test_tasks') as test_tasks:
            child_1()
            child_2()
            child_3()
        process_1(transaction_details['transaction_value']) >> test_tasks
        process_2(transaction_details['transaction_value']) >> test_tasks
        process_3(transaction_details['transaction_value']) >> test_tasks
        
    return process_tasks

def dynamic_process_tasks(transaction_details):
    with TaskGroup(group_id="dynamic_process_tasks", add_suffix_on_collision=True) as dynamic_process_tasks:
        with TaskGroup(group_id = 'test_tasks') as test_tasks:
            child_1()
            child_2()
            child_3()
        process_1(transaction_details['transaction_value']) >> test_tasks
        process_2(transaction_details['transaction_value']) >> test_tasks
        process_3(transaction_details['transaction_value']) >> test_tasks
        
    return dynamic_process_tasks
