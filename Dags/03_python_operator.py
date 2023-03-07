import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def say_hello_from_python_kwargs(name, email):
    print(f"Hello form python kwargs, received name={name}, received email={email}")

def say_hello_from_python_templates_dict(**context):
    print(f"Hello form python templates_dict, received name={context['templates_dict']['name']}, received email={context['templates_dict']['email']}")

def say_hello_from_python_opargs(*opargs):
    print(f"Hello form python opargs, received name={opargs[0]}, received email={opargs[1]}")


with DAG(
 dag_id="03_python_operator"
,start_date = datetime(2023,1,1)
,schedule="* * * * *"
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

   
    pytask_kwargs = PythonOperator(
         task_id = "pass_by_kwargs"
        ,python_callable = say_hello_from_python_kwargs
        ,op_kwargs={
             "name": "gavrilis"
            ,"email": "airflow@practice.com"
        }
    )


    pytask_template_dict = PythonOperator(
         task_id="pass_by_template_dict"
        ,python_callable = say_hello_from_python_templates_dict
        ,provide_context=True
        ,templates_dict = {
             "name": "gavrilis"
            ,"email": "airflow@practice.com"
        }
    )


    pytask_opargs = PythonOperator(
        task_id="pass_by_opargs"
        ,python_callable = say_hello_from_python_opargs
        ,op_args = ['gavrilis', 'airflow@practice.com']
    )


    end = DummyOperator(task_id="end_task")


    start >> pytask_kwargs >> pytask_template_dict >> pytask_opargs >> end