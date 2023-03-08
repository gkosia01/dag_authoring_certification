import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



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

def say_hello_from_python():
    print("Hello form python operator")

with DAG(
 dag_id="01_basic_dag_structure"
,schedule_interval="@daily"
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
        ,bash_command='echo "Hello form bash task" '
    )

    end = DummyOperator(task_id="end_task")


    start >> pytask >> bashtask >> end