import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from Subdags._10_subdags_child import subdag_factory
from airflow.operators.subdag import SubDagOperator

''' 
    In order to create subdags you must create a factory function that return a dag (the child)
    Then you have to use SubDagOperator to attach the subdag to the main dag
    The factory function should be in a different file than the main dag

    To the subdag factory function we send 
        as parent_id the partent dag_id
        as subdag_id the task id of the SubDagOperator
        and the default_args of the parent

    The main dag will shown the sub dag as a task. In order to see the subdag you have to click on the SubDagOperator task-> Zoom in 

'''

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


with DAG(
 dag_id="10_subdags__parent"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    subdag = SubDagOperator(
         task_id="subdag_task"
        ,subdag = subdag_factory("10_subdags__parent", "subdag_task", default_args)
    )

    end = DummyOperator(task_id="end_task")


    start  >> subdag >> end

