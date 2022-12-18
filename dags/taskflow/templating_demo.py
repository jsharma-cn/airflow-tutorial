import sqlite3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args= {'owner':'jsharma',
    'email':'my@gmail.com',
    'retries': 1}

def var_task_fun(name):
    print("From function param",name)

class CustopPostressOp(PostgresOperator):
    template_fields = ('sql', 'parameters')

with DAG(dag_id="templating_demo",
    default_args=default_args,
    start_date=datetime(2022,1,1),
    #end_date=datetime(2022,1,2),
    schedule_interval="@Daily",
    description="Templating Demo",
    max_active_runs=1,
    catchup=False,
    tags=['DE']
    ) as dag:
    
    my_var_task = PythonOperator(
        task_id='var_task',
        python_callable=var_task_fun,
        op_args = ["{{ var.json.my_param_json.name }}"]
    )

    postgres_op = CustopPostressOp(
        task_id= "postgres_op",
        #sql="select * from name where dt={{ds}}",
        sql ='sql/my_sql.sql',
        parameters= {
            'next_ds' : '{{next_ds}}',
            'prev_ds' : '{{prev_ds}}',
            'name' : '{{ var.json.my_param_json.name }}'
        }
    )

    my_var_task >>postgres_op