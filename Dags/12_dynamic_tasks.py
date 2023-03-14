import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import task

''' 
    The data of the dynamic tasks should be known when scheduler will parse the dag file in order to produce the dag
    For this reason we can use only static dictionary, variable or a connection to produce dynamic tasks
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

tasks_config = {
    "task_id_1": {
         "param1": "task1_param1_value"
        ,"param2": "task1_param2_value"
    }
    ,"task_id_2": {
         "param1": "task2_param1_value"
        ,"param2": "task2_param2_value"
    }
    ,"task_id_3": {
         "param1": "task3_param1_value"
        ,"param2": "task3_param2_value"
    }
}


with DAG(
 dag_id="12_dynamic_tasks"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    end = DummyOperator(task_id="end_task")

    for taskId, params in tasks_config.items():
        @task(task_id=f"{taskId}")
        def dynamic_task(taskId, param1, param2):
            print(f"I am task {taskId} and i have receive params {param1} and {param2}")

        start >> dynamic_task(taskId, params['param1'], params['param2']) >> end 
        

