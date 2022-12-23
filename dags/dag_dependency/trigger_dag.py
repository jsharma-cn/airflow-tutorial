from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 12, 20)
}

def _downloading():
    print('downloading')

with DAG('trigger_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    downloading = PythonOperator(
        task_id='downloading',
        python_callable=_downloading
    )

    # trigger_dag.py

    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target',
        trigger_dag_id='target_dag',
        wait_for_completion=True,
        execution_date='{{ds}}',
        poke_interval=60,
        reset_dag_run =True,
        failed_states= ['failed']
    )

downloading >> trigger_target