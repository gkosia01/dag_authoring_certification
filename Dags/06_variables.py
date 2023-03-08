import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable


''' 
    In UI we can specify variables Admin -> Variables
    Variables are Key-value pairs and they are stored in metadata database
    Airflow will hide the value in the UI and in the logs if the name of the variable contains the words secret or password
    If we have variables that they are used together it is a best practice to create the variables as dict to avoid multiple connections to the metadata database

    We can specify Environment variables by login into the scheduler container and execute 
        export AIRFLOW_VAR_<VARIABLE KEY>=<VARIABLE VALUE>
        to check the available variables execute env | grep  AIRFLOW_VAR_

        To create a glopal env variable go to docker-compose file and add the variables under scheduler and webserver processes

    We can use env variables to avoid many connections to the metadata database and to hide the variables because they are not visible through UI
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

def read_some_variables():
    conn_details = Variable.get('conn_details', deserialize_json=True)
    user = Variable.get('db_user')
    print(f"Server name is {conn_details['server']} and db name is {conn_details['db']}, user is {user}")

def read_variable_from_template(*opargs):
    print(f"Received variable value {opargs[0]}")


with DAG(
 dag_id="06_variables"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    read_some_variables = PythonOperator(
        task_id="read_some_variables"
       ,python_callable = read_some_variables

    )

    read_variable_templating = PythonOperator(
         task_id="read_variable_templating"
        ,python_callable = read_variable_from_template
        ,op_args = ["{{var.json.conn_details.server}}"]
    )


    end = DummyOperator(task_id="end_task")


    start  >> read_some_variables >> read_variable_templating >> end

