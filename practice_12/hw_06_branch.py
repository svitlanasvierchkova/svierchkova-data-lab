# Завдання 6 — розгалуження
from datetime import datetime
from airflow.sdk import dag, task
@dag(dag_id="hw_06_branch", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["homework"])
def hw_06_branch():
 @task
 def count_rows(): return 4820
 @task.branch
 def choose_path(rows): return "load_big" if rows>=1000 else ("load_small" if rows>0 else "no_data")
 @task
 def no_data(): print('no data')
 @task
 def load_small(): print('small')
 @task
 def load_big(): print('big')
 b=choose_path(count_rows()); b >> [no_data(), load_small(), load_big()]
hw_06_branch()