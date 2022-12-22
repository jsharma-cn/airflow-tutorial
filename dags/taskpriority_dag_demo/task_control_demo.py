from airflow import DAG
from datetime import datetime
import time
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain
default_args = {
    "start_date": datetime(2022,12,12)
}

with DAG(dag_id = "tasks_priority", 
    description="tasks priority dag",
    default_args = default_args,
    schedule_interval = "@daily",
    catchup = False,
    tags =["DE"],
    concurrency =2,
    max_active_runs=2) as dag:
    start = DummyOperator(task_id="start", pool='my_pool', priority_weight=10, weight_rule ='absolute')
    task2 = DummyOperator(task_id="task2", priority_weight=5, pool='my_pool', weight_rule ='absolute')
    task3 = DummyOperator(task_id="task3", priority_weight=4, pool='my_pool', weight_rule ='absolute')
    task4 = DummyOperator(task_id="task4",priority_weight=2, pool='my_pool', weight_rule ='absolute')
    stop = DummyOperator(task_id="stop", pool='my_pool')
    
    start >> task2 >> stop
    start >> task3 >> stop
    start >>task4 >> stop
     
    #cross_downstream([task1,task2,task3] , [task4,task5,task6])
    #chain(task1,[task2,task3], [task4,task5], task6)

