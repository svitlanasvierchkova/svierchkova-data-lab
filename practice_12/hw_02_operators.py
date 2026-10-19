# Завдання 2 — класичні оператори
from datetime import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def report(store, amount):
    print(f"Магазин: {store}, виручка: {amount}")

@dag(dag_id="hw_02_operators", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["homework"])
def hw_02_operators():
    start=EmptyOperator(task_id="start")
    show_date=BashOperator(task_id="show_date", bash_command="date")
    count_dags=BashOperator(task_id="count_dags", bash_command="ls -1 /opt/airflow/dags | wc -l")
    make_report=PythonOperator(task_id="make_report", python_callable=report, op_kwargs={"store":"Київ","amount":1810.0})
    finish=EmptyOperator(task_id="finish")
    start >> [show_date,count_dags] >> make_report >> finish
hw_02_operators()