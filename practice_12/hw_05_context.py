# Завдання 5 — контекст запуску і дата
from datetime import datetime               # імпорт модуля дати та часу
from airflow.sdk import dag, task, get_current_context # декоратори DAG/задачі та витягування контексту
from airflow.providers.standard.operators.bash import BashOperator # оператор для виконання Bash команд

@dag(
    dag_id="hw_05_context",                 # назва DAG
    start_date=datetime(2025,1,1),         # початкова дата
    schedule=None,                          # ручний запуск
    catchup=False,                          # без доганяння дат
    tags=["homework"]                       # мітка завдання
)
def hw_05_context():
    @task                                   # задача виведення контексту
    def show_context(): print(get_current_context()) # друк усього контексту запуску
    
    echo_date=BashOperator(task_id="echo_date", bash_command="echo {{ ds }}") # використання Airflow Jinja-шаблону для дати
    
    @task                                   # задача формування шляху
    def build_path():
        ld=get_current_context().get("logical_date") # витягуємо logical_date з контексту
        print(f"/data/sales/{ld.strftime('%Y-%m-%d') if ld else 'unknown_date'}.csv") # формування шляху до файлу за датою

    show_context() >> echo_date >> build_path() # порядок виконання задач

hw_05_context()                             # виклик DAG