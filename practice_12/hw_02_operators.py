# Завдання 2 — класичні оператори
from datetime import datetime                                 # імпорт модуля для роботи з датами
from airflow.sdk import dag                                    # декоратор для оголошення DAG
from airflow.providers.standard.operators.empty import EmptyOperator   # оператор-заглушка
from airflow.providers.standard.operators.bash import BashOperator     # оператор для виконання Bash-команд
from airflow.providers.standard.operators.python import PythonOperator # оператор для виконання Python-функцій

# Функція для використання в PythonOperator
def report(store, amount):
    print(f"Магазин: {store}, виручка: {amount}")              # друк звіту в лог

@dag(
    dag_id="hw_02_operators",                                  # унікальний ідентифікатор DAG
    start_date=datetime(2025,1,1),                             # дата початку розкладу
    schedule=None,                                             # запуск вручну
    catchup=False,                                             # не виправляти пропущені дати
    tags=["homework"]                                          # мітка для фільтрації
)
def hw_02_operators():
    start=EmptyOperator(task_id="start")                       # початкова задача
    show_date=BashOperator(task_id="show_date", bash_command="date") # виведення дати в системі
    count_dags=BashOperator(task_id="count_dags", bash_command="ls -1 /opt/airflow/dags | wc -l") # підрахунок файлів DAG
    make_report=PythonOperator(task_id="make_report", python_callable=report, op_kwargs={"store":"Київ","amount":1810.0}) # формування звіту
    finish=EmptyOperator(task_id="finish")                     # кінцева задача
    
    start >> [show_date,count_dags] >> make_report >> finish    # визначення залежностей між задачами

hw_02_operators()                                              # виклик DAG для реєстрації