import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
    By default a task will be executed when start_time + schedule has passed and all the upstream has completed

    Some times we want a task to be executed only under other conditions of the upstream
    
    all_success: (default) all parents have succeeded
    all_failed: all parents are in a failed or upstream_failed state
    all_done: all parents are done with their execution
    one_failed: fires as soon as at least one parent has failed, it does not wait for all parents to be done
    one_success: fires as soon as at least one parent succeeds, it does not wait for all parents to be done
    none_failed: all parents have not failed (failed or upstream_failed) i.e. all parents have succeeded or been skipped
    none_failed_or_skipped: all parents have not failed (failed or upstream_failed) and at least one parent has succeeded.
    none_skipped: no parent is in a skipped state, i.e. all parents are in a success, failed, or upstream_failed state
    dummy: dependencies are just for show, trigger at will

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

def raise_an_error():
    raise Exception()


with DAG(
 dag_id="14_trigger_rules"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    task1 = DummyOperator(task_id="task1")

    failedtask = PythonOperator(
         task_id = "failing_task"
        ,python_callable = raise_an_error
    )

    task3 = DummyOperator(task_id="task3", trigger_rule="one_success")

    task4 = DummyOperator(task_id="task4", trigger_rule="none_skipped")


    task5 = DummyOperator(task_id="task5", trigger_rule="none_failed_or_skipped")

    task6 = DummyOperator(task_id="task6", trigger_rule="always")


    start >> [task1,failedtask] >> task3 >> [task4,task5,task6]

