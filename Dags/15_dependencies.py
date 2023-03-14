import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import cross_downstream, chain


''' 
     << or t2.set_upstream(t1), >> or t1.set_downstream(t2) 
     one to multiple t1 >> [t2,t3,t4]

     Cross dependencies: 
        cross_downstream([t1,t2,t3], [t4,t5,t6])

        cross_downstream does not return anything so i cannot have  start >> cross_downstream([t1,t2,t3], [t4,t5,t6]) >> t7
        i should add  t4 >> t7
                      t5 >> t7
                      t6 >> t7

     Chain:
        chain(t1, [t2,t3], [t4,t5], t6)

        t1 has two arrows on two lists and then the lists link to t6
        the two lists must be the same size

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
 dag_id="15_dependencies"
,schedule_interval="@daily"
,default_args=default_args
,catchup=False
,tags=["DAG_AUTHORING_CERTIFICATION"]
) as dag:

    start = DummyOperator(task_id="start_task")

    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    t7 = DummyOperator(task_id="t7")
    t8 = DummyOperator(task_id="t8")
    t9 = DummyOperator(task_id="t9")
    t10 = DummyOperator(task_id="t10")
    t11 = DummyOperator(task_id="t11")


    end = DummyOperator(task_id="end_task")


    start >> t1 >> t2
    chain(t2,[t3,t4], [t5,t6],t7) 
    t7>> [t8,t9]
    cross_downstream([t8,t9], [t10,t11])
    t10 >> end
    t11 >> end
    