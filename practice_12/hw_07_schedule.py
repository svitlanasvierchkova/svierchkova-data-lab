# Завдання 7 — розклад
from datetime import datetime               # імпорт класу дати
from airflow.sdk import dag, task, get_current_context # інструменти airflow.sdk

@dag(
    dag_id="hw_07_schedule",                # унікальна назва DAG
    start_date=datetime(2025,1,1),         # з якої дати рахується розклад
    schedule="*/5 * * * *",                 # запуск кожні 5 хвилин за cron-виразом
    catchup=False,                          # пропущені запуски не надолужуються
    max_active_runs=1,                      # максимум 1 активний запуск одночасно
    tags=["homework"]                       # тег у списку
)
def hw_07_schedule():
    @task                                   # періодична задача
    def heartbeat():
        c=get_current_context()             # отримання поточного контексту
        print(c['logical_date'])            # виведення логічної дати запуску
        print(c['dag_run'].run_type)        # виведення типу запуску (наприклад, scheduled)

    heartbeat()                             # запуск задачі

hw_07_schedule()                            # ініціалізація DAG