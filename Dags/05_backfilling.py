import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


''' 
    The backfilling allows to re run previous schedules from start date
        We can set the catchup=True to cover past runs
        We can clear the state of the dag run from UI in order to reschedule it automatically
        Using the airflow CLI we can execute a dag for previous runs(on scheduler container):  airflow dags backfill -s <start_date> -e <end_Date> <dag_id>
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
 dag_id="05_backfilling"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")


    end = DummyOperator(task_id="end_task")


    start  >> end

