import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
    Scheduler logic:
        For each task
            Check if interval has passed and all upstream tasks has completed
                Add the task to execution queue

    start_date: the datetime dag starts to scheduled   (datetime object)
    schedule  : the frequency the dag will run (timedelta or cron or macros)
                       default "@daily"
    The scheduler will trigger the dag run when start_date + schedule_interval pass and will have execution date the start of the interval
        start_date = 2023-01-01 10:00:00
        schedule   = 10 mins

        1st run: 2023-01-01 10:10:00 with execution date 2023-01-01 10:00:00
        2nd run: 2023-01-01 10:20:00 with execution date 2023-01-01 10:10:00

    Cron expression:
    * * * * *: minute hour day_of_month month day_of_week

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


def say_hello_from_python(execution_date):
    print(f"Hello form python operator executed at {execution_date}")

with DAG(
 dag_id="02_scheduling_dag"
,schedule="* * * * *"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    pytask = PythonOperator(
         task_id = "python_task"
        ,python_callable = say_hello_from_python
    )

    bashtask = BashOperator(
         task_id="bash_task"
        ,bash_command='echo "Hello form bash task, executed at {{ds}}" '
    )

    end = DummyOperator(task_id="end_task")


    start >> pytask >> bashtask >> end