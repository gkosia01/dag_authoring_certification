import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

''' 
    trigger_dag_id
    execution_date: string or datetime object  trigger as of date (backfilling)
    wait_for_completion= True/False
    poke_interval time to check if completed (default 60 seconds)
    reset_dag_run=True/False (default True): in airflow cannot have two dag runs for the same execution date so this property will set the current execution as of that execution date run
    failed_states = [], the states of the child considered failed

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
 dag_id="27_TriggerRunOperator"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    call_other_dag = TriggerDagRunOperator(
         task_id="call_other_dag"
        ,trigger_dag_id = "25_Dag_versioning_and_Dynamic_dags"
        ,execution_date = datetime(2023,1,2)
        ,reset_dag_run=True
    )
    end = DummyOperator(task_id="end_task")


    start  >> call_other_dag >> end

