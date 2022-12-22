from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream, chain
default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 0}

def t1_func():
    print('T1 function is called')

def t2_func():
    print('T2 function is called') 
    raise Exception("Exception occured")

def _success_func(context):
    print("Wow, I am called", context)

def _failed_func(context):
    print("Wow, I am called",context) 

def _retry_func(context):
    print("Wow, I am called",context) 

with DAG(dag_id="callback",
    default_args=default_args,
    start_date=datetime(2022,1,1),
    schedule_interval="@Daily",
    description="Variable Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE'],
    on_success_callback=_success_func,
    on_failure_callback=_failed_func
    ) as dag:
    
    t1 = PythonOperator(
        task_id='t1',
        python_callable=t1_func,
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
        on_success_callback=_success_func,
        on_failure_callback=_failed_func,
        on_retry_callback=_retry_func
    )

    t2 = PythonOperator(
        task_id='t2',
        retries=0,
        python_callable=t2_func,
        on_success_callback=_success_func,
        on_failure_callback=_failed_func,
        on_retry_callback=_retry_func
    )
    start = DummyOperator(task_id="start")
    stop = DummyOperator(task_id="stop")
    
    chain(start, [t1,t2], stop)