import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.sensors.date_time import DateTimeSensor

''' 
    Sensors are tasks that waits for a condition to occur
    The task sensor pokes and check the condition

    poke_interval= n : check every n seconds
    timeout: if the condition never met when to timeout
    mode: poke or reschedule: when reschedule the task will be rescheduled after n seconds and will release the slot
    soft_fail: when timeout will go to status skiped instead of fail
    exponential_backoff: when true each time will wait larger interval for poke

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
 dag_id="21_Sensors"
,schedule_interval=timedelta(minutes=2)
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    sensor = DateTimeSensor(
         task_id="sensor"
        ,target_time="{{execution_date.add(seconds=90)}}"
        ,poke_interval=5
        ,mode="poke"
    )
    
    end = DummyOperator(task_id="end_task")


    start  >> sensor >> end

