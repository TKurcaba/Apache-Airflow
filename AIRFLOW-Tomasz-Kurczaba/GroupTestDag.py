import datetime

from airflow import DAG
from subDagGroupTest import subdag
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
   
DAG_NAME = 'example_subdag_operator'


def create_section():
    """
    Create tasks in the outer section.
    """
    dummies = [DummyOperator(task_id=f'task-{i + 1}') for i in range(5)]


with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@once",
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', dag.default_args),
    )

    some_other_task = DummyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', dag.default_args),
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end
    
    start1 = DummyOperator(task_id="start1")
 
    with TaskGroup("section_1", tooltip="Tasks for Section 1") as section_11:
        create_section()
 
    some_other_task1 = DummyOperator(task_id="some-other-task1")
    with TaskGroup("section_2", tooltip="Tasks for Section 2") as section_21:
        create_section()
 
    end1 = DummyOperator(task_id='end1')
    start1 >> section_11 >> some_other_task1 >> section_21 >> end1