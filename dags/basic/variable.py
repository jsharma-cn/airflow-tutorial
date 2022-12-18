from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args= {'owner':'jsharma',
    'email':'my@gmail.com'}

def var_task_fun():
    param= Variable.get('my_param')
    print("My variable paramter",param)

with DAG(dag_id="varible_demo",
    default_args=default_args,
    start_date=datetime(2022,1,1),
    end_date=datetime(2022,1,2),
    schedule_interval="@Daily",
    description="Variable Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    ) as dag:
    
    my_var_task = PythonOperator(
        task_id='var_task_id',
        python_callable=var_task_fun
    )