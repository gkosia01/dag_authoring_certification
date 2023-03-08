import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

''' 
    Each operator have specific parameters that allows for templating in definition of operator filed template_fields
    To add jinja template variable use the {{ }}

    macros: 
        execution_date: yyyy-MM-ddThh:mm:ss
        ds: date of dag run as YYYY-MM-DD
        prev_ds
        next_ds
        dag: the current dag object (dag.dag_id ...)
        params: access the partams specified in the params parameter of the operator
                in operator params= {"par1": "parvalue"}, in tamplating {{params.par1}}
        var.value.<key>: reference to a variable
        var.json.<key>.<property>: reference to a json variable
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

def print_some_macros(*opargs):
    print(f"0: {opargs[0]}, 0: {opargs[1]}, 0: {opargs[2]}")


with DAG(
 dag_id="07_templating"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    task_bash = BashOperator(
         task_id= "task_bash"
        ,bash_command= 'echo "Running from dag {{dag.dag_id}} with params {{params.param_value}}"'
        ,params={"param_value": "value of param"}
    )

    task_python= PythonOperator(
         task_id = "task_python"
        ,python_callable = print_some_macros
        ,op_args = ["{{execution_date}}", "{{ds}}", "{{dag.schedule_interval}}"]
    )


    end = DummyOperator(task_id="end_task")


    start  >> task_bash >> task_python >> end

