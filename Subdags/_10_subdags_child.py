import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    The factory function should return a dag
    The dag_id should be {parent_id}.{subdag_id}

    To read the xcoms of the parent into the child use the get_current_context()['ti'] to pull the xcoms
'''

def subdag_factory(parent_id, subdag_id, default_args):
    with DAG(
    dag_id=f"{parent_id}.{subdag_id}"
    ,schedule_interval="@daily"
    ,default_args=default_args
    ,catchup=False
    ,tags=["DAG_AUTHORING_CERTIFICATION"]
    ) as dag:

        task1 = DummyOperator(task_id="task1")


        task2 = DummyOperator(task_id="task2")


        task1  >> task2
    
    return dag

