import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    If we want the current task to be executed only if at the prev dag run the downstream is completed
        wait_for_downstream = True
'''

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3, 6),
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
 dag_id="20_wait_for_downstream"
,schedule_interval="@daily"
,default_args=default_args
,catchup=True
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    t1 = DummyOperator(task_id="t1",wait_for_downstream = True )
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")

    end = DummyOperator(task_id="end_task")


    start >> t1 >> t2 >> t3 >>  end

