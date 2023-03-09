import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    concurrency on airflow instance level:
        Number of concurrent tasks for a specific dag:    DAG_CONCURRENCY default: 16
        Number of concurrent task runs on the instance:   PARALLELISM default: 32
        Number of concurrent dag runs for a specific dag: MAX_ACTIVE_RUNS_PER_DAG = 16

    concurrency on dag level:
        concurrency = n:  execute at most n tasks concurrent accross all active dag runs of the dag
        max_active_runs= n: n concurrent dag runs
    
    concurrency on task level:
        task_concurrency = n: n task runs across all dag runs of the dag
        pool = 'default_pool': on which pool to run the task
        pool_slots=n the task will take n slots to executed

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
 dag_id="16_concurrency"
,schedule_interval="@daily"
,default_args=default_args
,catchup=True
,tags=["DAG_AUTHORING_CERTIFICATION"]
,concurrency = 3
,max_active_runs = 3
) as dag:

    start = DummyOperator(task_id="start_task", task_concurrency = 1)


    end = DummyOperator(task_id="end_task")


    start  >> end

