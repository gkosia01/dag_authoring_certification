import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    Timeout on dag level: dagrun_timeout = timedelta(minutes=10)
    Timeout on task level: execution_timeout= timedelta(seconds=10)
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


with DAG(
 dag_id="22_timeouts"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,dagrun_timeout = timedelta(minutes=10)
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task", execution_timeout= timedelta(seconds=10))


    end = DummyOperator(task_id="end_task", execution_timeout= timedelta(seconds=10))


    start  >> end

