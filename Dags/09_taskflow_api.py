import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from typing import Dict

''' 
    Taskflow api provide decorators to make the code size smaller and clener
    We can use Taskflow api to handle beteer the xcoms 
    Currently the following decorators are supported
        @task.python: a python operator task
        @task.virtualenv: execute a python operator to a new env
        @task_group: group of tasks

    
    To access the context data in a python task use the method get_current_context
    If the method return multiple xcoms there are two way to specify it
        By using the Dict [] and show the returned values ex. Dict[str,str]
        By set the task parameter multiple_outputs=True

        Because ther returned values are stored in the variable if we dont want to write them to the metadata database then specify the task parameter do_xcom_push=False
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


@task
def say_hello():
    print("Hello form a python function")

    context = get_current_context()
    print(context)

@task(do_xcom_push=False) # multiple_outputs=True
def produce_some_xcoms() -> Dict[str,str] :
    return {"first_xcom": "value_of_first_xcom", "sec_xcom": "value_of_sec_xcom"}

@task
def consume_some_xcoms(first_xcom, sec_xcom):
    print(f"Received {first_xcom} and {sec_xcom}")

@dag(
    dag_id="09_taskflow_api"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"])
def my_dag():

    start = DummyOperator(task_id="start_task")

    end = DummyOperator(task_id="end_task")

    produced_values = produce_some_xcoms()

    start >> say_hello() >> consume_some_xcoms(produced_values['first_xcom'], produced_values['sec_xcom']) >> end


my_dag()