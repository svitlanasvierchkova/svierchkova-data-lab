# Завдання 1 — перший пайплайн
from datetime import datetime               # імпортуємо клас datetime для вказання стартової дати DAG

from airflow.sdk import dag, task           # імпортуємо декоратори dag та task з airflow.sdk

# Вихідні дані з продажами по магазинах
SALES = [
    {"store": "Київ", "amount": 1200.0},
    {"store": "Львів", "amount": 850.5},
    {"store": "Одеса", "amount": 430.25},
    {"store": "Київ", "amount": 610.0},
]

@dag(
    dag_id="hw_01_pipeline",                # назва DAG в інтерфейсі
    start_date=datetime(2025, 1, 1),        # з якої дати рахуються запуски
    schedule=None,                          # запуск тільки вручну
    catchup=False,                          # не доганяти пропущені дати
    tags=["homework"],                      # мітка для фільтра у списку
)
def hw_01_pipeline():
    @task                                   # задача видобування даних
    def extract():
        print(f"Кількість записів: {len(SALES)}")  # виводимо кількість записів у лог
        return SALES                        # повертаємо дані для наступної задачі

    @task                                   # задача трансформації даних
    def transform(rows):
        total = round(sum(item['amount'] for item in rows), 2)  # рахуємо загальну суму продажів
        stores = len({item['store'] for item in rows})         # рахуємо кількість унікальних магазинів
        return {'total': total, 'stores': stores, 'rows': len(rows)}  # повертаємо агреговані результати

    @task                                   # задача завантаження/виведення даних
    def load(data):
        print(data)                         # виводимо отриманий результат у лог

    load(transform(extract()))              # послідовне виконання задач і передача даних

hw_01_pipeline()                            # без цього рядка DAG не з'явиться