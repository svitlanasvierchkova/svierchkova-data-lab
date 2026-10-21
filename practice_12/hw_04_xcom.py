# Завдання 4 — XCom
from datetime import datetime               # імпорт модуля роботи з датами
from airflow.sdk import dag, task, get_current_context # інструменти для роботи з DAG та контекстом

@dag(
    dag_id="hw_04_xcom",                    # унікальний id
    start_date=datetime(2025,1,1),         # дата початку
    schedule=None,                          # без розкладу
    catchup=False,                          # відключено catchup
    tags=["homework"]                       # тег завдання
)
def hw_04_xcom():
    @task                                   # задача підготовки файлу
    def prepare_file():
        ti=get_current_context()["ti"]      # отримання екземпляра task instance з контексту
        ti.xcom_push(key="file_name", value="sales_2025_03.csv") # відправка ім'я файлу в XCom
        ti.xcom_push(key="row_count", value=4820)                # відправка кількості рядків в XCom

    @task                                   # задача перевірки даних з XCom
    def check_file():
        ti=get_current_context()["ti"]      # отримання екземпляра task instance
        print(ti.xcom_pull(task_ids="prepare_file", key="file_name"), ti.xcom_pull(task_ids="prepare_file", key="row_count")) # отримання та виведення даних з XCom

    prepare_file() >> check_file()          # встановлення послідовності виконання

hw_04_xcom()                                # виклик DAG