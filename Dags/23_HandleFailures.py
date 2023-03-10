import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    At dag level:
        on_success_callback: callable_function(context), 
        on_failure_callback: 
    At task level:
        on_success_callback: callable_function(context)
        on_failure_callback: isinstance(context['exception'], AirflowTaskTimeout)
        on_retry_callback: context['ti'].try_number
'''

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def throw_an_exception():
    raise Exception()

def handle_dag_failure(context):
    print(f"Dag failed with exception {context['exception']}")


def retry_failed_task(context):
    print(f"Handle retry run number {context['ti'].try_number}")

def handle_task_failure(context):
    print("Handle task failure")

with DAG(
 dag_id="23_HandleFailures"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,on_failure_callback = handle_dag_failure
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    raise_an_exception = PythonOperator(
         task_id = "raise_an_exception"
        ,python_callable= throw_an_exception
        ,on_retry_callback = retry_failed_task
        ,on_failure_callback = handle_task_failure
        ,retry_delay= 20
        ,retries = 1
    )
    end = DummyOperator(task_id="end_task")


    start  >> raise_an_exception >> end

