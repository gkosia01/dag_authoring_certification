import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import time

''' 
    SLAs is the time suppose to take for a task or dag to be executed
    Using the SLAs we can notified when a task run or dag run takes longer than expected 

    The SLAs dont work if the trigger is manual 

    The SLA count from the execution time of the dag
        sla= timedelta(minutes=5): So if i have task t1 that need 3 mins to complete and t2 that needs 2 mins then the SLA for task 2 must be  >5 

    On dag level sla_miss_callback: the parameter receive a callback function that will be called when we miss the sla
                                    func_name(dag, task_list, blocking_task_list, slas, blocking_tis)
                                    dag: dag info
                                    task_list: tasks instance list
                                    blocking_task_list: the task that miss the sla
                                    blocking_tis: list of the tasks that didnt executed because we miss the sla
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

def wait_for_some_time():
    time.sleep(50)

def handle_missed_sla(dag, task_list, blocking_task_list, slas, blocking_tis):
    print("dag:"  + str(dag))
    print("task_list:" + str(task_list))
    print("blocking_task_list: " + str(blocking_task_list))
    print("slas: " + str(slas))
    print("blocking_tis: " + str(blocking_tis))
    
    
with DAG(
 dag_id="24_SLAs"
,schedule=timedelta(minutes=2)
,default_args=default_args
,catchup=False
,sla_miss_callback = handle_missed_sla
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    t1 = PythonOperator(
         task_id="task_miss_sla"
        ,python_callable = wait_for_some_time
    )

    t2 = DummyOperator(task_id="t2"
        ,sla= timedelta(seconds=49))

    end = DummyOperator(task_id="end_task")


    start >> t1 >> t2  >> end

