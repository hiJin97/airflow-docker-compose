import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id='dags_task_link_operator',
        schedule='0 0 * * *',
        start_date=pendulum.datetime(2024, 7, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['task_linking'],
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    bash_t1 = BashOperator(task_id='bash_t1', bash_command='echo "t1"')
    bash_t2 = BashOperator(task_id='bash_t2', bash_command='echo "t2"')
    bash_t3 = BashOperator(task_id='bash_t3', bash_command='echo "t3"')
    bash_t4 = BashOperator(task_id='bash_t4', bash_command='echo "t4"')
    bash_t5 = BashOperator(task_id='bash_t5', bash_command='echo "t5"')
    bash_t6 = BashOperator(task_id='bash_t6', bash_command='echo "t6"')
    bash_t7 = BashOperator(task_id='bash_t7', bash_command='echo "t7"')
    bash_t8 = BashOperator(task_id='bash_t8', bash_command='echo "t8"')
    end_task = EmptyOperator(task_id='end_task')

    start_task >> bash_t1 >> [bash_t2, bash_t3] >> bash_t4
    bash_t5 >> bash_t4
    [bash_t4, bash_t7] >> bash_t6 >> bash_t8 >> end_task
