
import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='dags_bash_shell_script',
    schedule='0 0 * * 6#1',
    start_date=pendulum.datetime(2024, 7, 1, tz='Asia/Seoul'),
    catchup=False,
    tags=['run_shell_script'],
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    t1_orange = BashOperator(
        task_id='t1_orange',
        bash_command='$AIRFLOW_HOME/plugins/shell/select_fruit.sh ORANGE'
    )

    t2_avocado = BashOperator(
        task_id='t2_avocado',
        bash_command='$AIRFLOW_HOME/plugins/shell/select_fruit.sh AVOCADO'
    )
    end_task = EmptyOperator(task_id='end_task')

    start_task >> t1_orange >> t2_avocado >> end_task

