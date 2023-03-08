import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
    Xcoms can be use to exchange small amount of data between the tasks
    They are stored in teh metadata database 
    To access a specific xcom you need the ti (task instance)

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

def produce_some_xcoms(ti):
    ti.xcom_push(key='xcom_from_producer', value='producer_value')

    return {"xcom_produced_by_return_1": "xcom_produced_by_return_value1", "xcom_produced_by_return_2": "xcom_produced_by_return_value2"}

def consume_the_xcoms(ti):
    xcom_value = ti.xcom_pull(key="xcom_from_producer", task_ids="xcom_producer")
    print(f"Consume the xcom xcom_from_producer with value {xcom_value}")

    xcoms_returned = ti.xcom_pull(task_ids="xcom_producer")
    print(f"Xcoms returned from producer {xcoms_returned['xcom_produced_by_return_1']} and {xcoms_returned['xcom_produced_by_return_2']}" )


with DAG(
 dag_id="08_xcoms"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    xcom_producer = PythonOperator(
         task_id = "xcom_producer"
        ,python_callable = produce_some_xcoms
    )

    xcom_consumer = PythonOperator(
         task_id="xcom_consumer"
        ,python_callable=consume_the_xcoms
    )
    end = DummyOperator(task_id="end_task")


    start  >> xcom_producer >> xcom_consumer >> end

