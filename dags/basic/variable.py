from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 1}

def var_task_fun(name):
    param= Variable.get('my_param')
    #param_docker= Variable.get('MY_DATA',deserialize_json=True)
    #print('From docker env file',param_docker['name'])
    # To fetch jason value
    param_json= Variable.get('my_param_json', deserialize_json=True)
    print("My variable paramter",param_json['address'])
    print("From function param",name)

with DAG(dag_id="varible_demo",
    default_args=default_args,
    start_date=datetime(2022,1,1),
    #end_date=datetime(2022,1,2),
    schedule_interval="@Daily",
    description="Variable Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    ) as dag:
    
    my_var_task = PythonOperator(
        task_id='var_task',
        python_callable=var_task_fun,
        op_args = ["{{ var.json.my_param_json.name }}"]
    )

    dummy_task = DummyOperator(task_id="dummy_task")
    dummy_task >> my_var_task