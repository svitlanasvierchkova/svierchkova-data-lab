# Завдання 1 — перший пайплайн

from datetime import datetime
from airflow.sdk import dag, task

SALES = [
    {"store": "Київ", "amount": 1200.0},
    {"store": "Львів", "amount": 850.5},
    {"store": "Одеса", "amount": 430.25},
    {"store": "Київ", "amount": 610.0},
]

@dag(
    dag_id="hw_01_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["homework"],
)
def hw_01_pipeline():
    @task
    def extract():
        print(f"Кількість записів: {len(SALES)}")
        return SALES

    @task
    def transform(rows):
        total = round(sum(item['amount'] for item in rows), 2)
        stores = len({item['store'] for item in rows})
        return {'total': total, 'stores': stores, 'rows': len(rows)}

    @task
    def load(data):
        print(data)

    load(transform(extract()))

hw_01_pipeline()
