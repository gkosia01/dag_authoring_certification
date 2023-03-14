import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    If we want to run the task only if at its previous execution completed succesfully or skipped
    We can use depends_on_past=True to force airflow to check first its previous execution

        As best practice set the retries=0 because if at the previous run failed then airflow will always retry to execute the task 
                    also set the execution_timeout in case the previous run failed the current will run for ever with no status
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

def raise_an_exception():
    raise Exception()

with DAG(
 dag_id="19_depends_on_past"
,schedule_interval="@daily"
,default_args=default_args
,catchup=True
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    depend_on_prev = PythonOperator(
         task_id="depend_on_prev"
        ,python_callable = raise_an_exception
        ,depends_on_past = True
        ,retries=0
        ,execution_timeout=timedelta(seconds=30)
    )

    end = DummyOperator(task_id="end_task")


    start  >>  end

