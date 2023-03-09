from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

'''
    The task group file should contain a function that creates and return a taskgroup
    In Taskflow api there is also the @task_group decorator

'''


def group_factory():
    with TaskGroup(
         group_id = "a_small_group"
    ) as group_task:

        start_of_group = DummyOperator(task_id="start_of_group")
        end_of_group = DummyOperator(task_id="end_of_group")

        start_of_group >> end_of_group
    
    return group_task
    