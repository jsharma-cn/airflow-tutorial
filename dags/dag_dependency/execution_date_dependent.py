from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args ={
    "owner":"jsharma",
    "retries":0
}

def _my_func():
    context = get_current_context()
    print('My dependent  function is called', context['execution_date'])

with DAG(dag_id="dependent_dag", 
    start_date=datetime(2022,12,15),
    catchup=True,
    schedule_interval="@Daily",
    default_args=default_args,
    tags=["DE"]
    ) as dag:

    external_task_op = ExternalTaskSensor(task_id="waiting_task",
        external_dag_id="MyParentDag",
        external_task_id='my_func',
        failed_states=['failed','skipped'],
        allowed_states=['success']
        )
    
    myOp = PythonOperator(task_id='my_func',
        python_callable=_my_func
    )

    stop= DummyOperator(task_id = 'stop')

myOp >> external_task_op >> stop



    