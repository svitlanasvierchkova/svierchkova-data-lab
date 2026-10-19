# Завдання 4 — XCom
from datetime import datetime
from airflow.sdk import dag, task, get_current_context
@dag(dag_id="hw_04_xcom", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["homework"])
def hw_04_xcom():
 @task
 def prepare_file():
  ti=get_current_context()["ti"]; ti.xcom_push(key="file_name", value="sales_2025_03.csv"); ti.xcom_push(key="row_count", value=4820)
 @task
 def check_file():
  ti=get_current_context()["ti"]; print(ti.xcom_pull(task_ids="prepare_file", key="file_name"), ti.xcom_pull(task_ids="prepare_file", key="row_count"))
 prepare_file() >> check_file()
hw_04_xcom()