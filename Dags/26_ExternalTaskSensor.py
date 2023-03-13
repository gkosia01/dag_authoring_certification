import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor

''' 
    The idea of the ExternalTaskSensor is to wait for a task in another dag to be completed before you continue
    The extrernal task should have the same schedule id as the waiting task

    If the external task have different schedule we han use the  execution_delta=  timedelta() to check some period back
        execution_delta=  timedelta(): same start date different schedule
    Another way is to use the execution_date_fn = <function>(execution_date) that will return a list of accepted execution dates for the external task
        execution_date_fn = <function>(execution_date): same schedule but different start date
    To check for the external task status we can use the parameters failed_states=[<list of status consider the external task as fail>], allowed_states=[<list of status consider the external task as succed>]

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
 dag_id="26_ExternalTaskSensor"
,schedule=timedelta(minutes=3)
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    wait_for_external = ExternalTaskSensor(
         task_id="wait_for_external"
        ,external_dag_id = "25_Dag_versioning_and_Dynamic_dags"
        ,external_task_id= "end_task"
        ,execution_delta=  timedelta(minutes=1)
    )
    end = DummyOperator(task_id="end_task")


    start  >> wait_for_external >> end

