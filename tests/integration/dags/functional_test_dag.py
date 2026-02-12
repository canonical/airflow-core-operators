from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_functional_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    BashOperator(task_id="ping", bash_command="echo pong")
