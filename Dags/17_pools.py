import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
    By default all the tasks in airflow run on the default_pool witch has 128 slots
    We can create pools from UI->Admin->Pools in order to isolate specific tasks to run on specific pool 
    For a task to run on a specific pool:
         We must use the parameter pool='pool name' 
         Optional set the parameter pool_slots=n if we want to reserve n slots for execution

'''

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3, 1),
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


with DAG(
 dag_id="17_pools"
,schedule_interval="@daily"
,default_args=default_args
,catchup=True
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    ingest_data = DummyOperator(task_id="ingest_data", pool='ingest_pool', pool_slots=6)

    end = DummyOperator(task_id="end_task")


    start >> ingest_data >> end

