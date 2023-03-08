import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    Deterministic:  for the same input produce the same output
    Idemponent:     if executed multiple times with the same input
                        Does not fail
                        Does not produce duplicate data
                        Has the same effects as running it once
    Atomic:         All operations will success or all will fail

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

'''
    The calculate_stats_and_notify is not atomic becasue the process of data might succed but the send email might fail
    A better design is to split the two functionalities into two tasks
        process_data
        send email
'''
def calculate_stats_and_notify():
    print("Process some data")
    print("Send the results")


'''
    The append_new_data is not idemponent because if we rerun the process for backfilling or retry the task will produce duplicates
    A better solution is to check for the existance of new records or to overwrite them (Merge or Upsert)
'''
def append_new_data():
    print("Append new records to file")


with DAG(
 dag_id="04_Idemponent_Deterministic_Atomic"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    calculate_stats_send_email = PythonOperator(
         task_id= "calculate_stats_send_email"
        ,python_callable= calculate_stats_and_notify
    )


    insert_new_data = PythonOperator(
         task_id = "insert_new_data"
        ,python_callable = append_new_data
    )

    end = DummyOperator(task_id="end_task")


    start  >> calculate_stats_send_email >> insert_new_data >> end

