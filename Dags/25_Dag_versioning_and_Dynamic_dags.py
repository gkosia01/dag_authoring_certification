import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    Dag versioning:

        If a new task added then this new task will have no status for the historic dag runs
        If a task is deleted then its historical runs will not be visible from UI 

        When we make changes to the dags the history of the dag is corrupted
        As a best practive to have the correct history we create a new dag each time with the version as suffix _v_0_0_1

    Dynamic dags:

        1st way: Generate dags based on a dictionary configuration
        2nd way: Crate a template file with the dag properties
                 Create a script that read the template and create the dag

'''

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 1, 1, 0, 0, 0),
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
 dag_id="25_Dag_versioning_and_Dynamic_dags"
,schedule=timedelta(minutes=2)
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")


    end = DummyOperator(task_id="end_task")


    start  >> end

