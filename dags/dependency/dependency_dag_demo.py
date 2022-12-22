from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain
default_args = {
    "start_date": datetime(2022,12,12)
}

with DAG(dag_id = "dependecny_tasks", 
    description="dependency dag",
    default_args = default_args,
    schedule_interval = "@daily",
    catchup = False,
    tags =["DE"]) as dag:
    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")
    task6 = DummyOperator(task_id="task6")

    #[task1,task2,task3] >> task6
    #cross_downstream([task1,task2,task3] , [task4,task5,task6])
    chain(task1,[task2,task3], [task4,task5], task6)

