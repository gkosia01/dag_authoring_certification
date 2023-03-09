import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    By default all the tasks have the same priority
    Worker nodes will pick one from execution queue and execute it

    If we want to give priority to a task we can use the  priority_weight parameter.
    The higher the number is, the higher the priority

    We can use also the priority_rule
        downstream (default): the sum of priority_weight of the downstream
        upstream: the sum of priority_weight of the upstream
        absolute: baseed on the priority_weight of the task

        The idea behind the priority_rule is to give priotiry to the tasks of downstream or upstream in case we have multiple dag runs
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
 dag_id="18_tasks_priority"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    t1 = DummyOperator(task_id="t1", priority_weight = 1,pool='singleslot')
    t2 = DummyOperator(task_id="t2", priority_weight = 2,pool='singleslot')
    t3 = DummyOperator(task_id="t3", priority_weight = 3,pool='singleslot')
    t4 = DummyOperator(task_id="t4", priority_weight = 4,pool='singleslot')
    t5 = DummyOperator(task_id="t5", priority_weight = 1,pool='singleslot')

    end = DummyOperator(task_id="end_task")


    start >> [t1,t2,t3,t4,t5] >> end

