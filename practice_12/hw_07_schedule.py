# Завдання 7 — розклад
from datetime import datetime
from airflow.sdk import dag, task, get_current_context
@dag(dag_id="hw_07_schedule", start_date=datetime(2025,1,1), schedule="*/5 * * * *", catchup=False, max_active_runs=1, tags=["homework"])
def hw_07_schedule():
 @task
 def heartbeat():
  c=get_current_context(); print(c['logical_date']); print(c['dag_run'].run_type)
 heartbeat()
hw_07_schedule()