from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.models.baseoperator import chain
import time
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 6, 7),
}

dag = DAG(dag_id="chain",
    default_args=default_args,
    schedule_interval="@daily",
    concurrency = 5,
    max_active_runs=1
)

month_lst_ds = ['Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov']

start_op = DummyOperator(task_id='start_task', dag=dag)
end_op = DummyOperator(task_id='end_task', dag=dag)
for month_ds in month_lst_ds:
    month_op = DummyOperator(task_id=f"dummy_start_trx_table_imports_{month_ds}", dag=dag)
    chain(start_op , month_op , end_op)