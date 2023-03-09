import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator

''' 
    The callable function of the BranchOperator should return the task_id of the task to be executed
    The downstream of the branch will not be executed because the rest of the tasks in the branch are skipped
    To resolve this issue we should change the trigger_rule of the downstream tasks to none_failed_or_skipped

    Other branch operators are BranchSQLOperator, BranchDatetimeOperator, BranchDayOfWeekOperator

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

def choose_choice():
    if 1==2:
        return "choice_01"
    elif 1==3:
        return "choice_02"
    else:
        return "choice_03"

with DAG(
 dag_id="13_branching"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    choice_01 = DummyOperator(task_id="choice_01")
    
    choice_02 = DummyOperator(task_id="choice_02")
    
    choice_03 = DummyOperator(task_id="choice_03")


    branch = BranchPythonOperator(
         task_id="check_choice"
        ,python_callable=choose_choice
    )


    end = DummyOperator(task_id="end_task", trigger_rule = "none_failed_or_skipped")


    start  >> branch >> [choice_01, choice_02, choice_03] >> end

