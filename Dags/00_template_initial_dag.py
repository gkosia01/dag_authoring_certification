import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



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
 dag_id="00_template_initial_dag"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")


    end = DummyOperator(task_id="end_task")


    start  >> end

