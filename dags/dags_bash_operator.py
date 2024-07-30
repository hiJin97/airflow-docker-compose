import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id='dags_bash_operator',
        schedule='0 0 * * *',
        start_date=pendulum.datetime(2024, 7, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['bash_operator'],
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    bash_t1 = BashOperator(task_id='bash_t1', bash_command='echo whoami')
    bash_t2 = BashOperator(task_id='bash_t2', bash_command='echo $HOSTNAME')
    end_task = EmptyOperator(task_id='end_task')

    start_task >> bash_t1 >> bash_t2 >> end_task
