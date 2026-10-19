# Завдання 5 — контекст запуску і дата
from datetime import datetime
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
@dag(dag_id="hw_05_context", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["homework"])
def hw_05_context():
 @task
 def show_context(): print(get_current_context())
 echo_date=BashOperator(task_id="echo_date", bash_command="echo {{ ds }}")
 @task
 def build_path():
  ld=get_current_context().get("logical_date"); print(f"/data/sales/{ld.strftime('%Y-%m-%d') if ld else 'unknown_date'}.csv")
 show_context() >> echo_date >> build_path()
hw_05_context()