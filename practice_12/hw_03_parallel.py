# Завдання 3 — паралельні гілки
from datetime import datetime               # імпорт модуля роботи з датами
from airflow.sdk import dag, task           # імпорт основних декораторів

@dag(
    dag_id="hw_03_parallel",                # назва DAG
    start_date=datetime(2025,1,1),         # стартова дата
    schedule=None,                          # запуск вручну
    catchup=False,                          # пропущені запуски не виконуються
    tags=["homework"]                       # мітка списку
)
def hw_03_parallel():
    @task                                   # початкова задача
    def start(): print("Старт")
    
    @task                                   # завантаження продажів
    def load_sales(): return 1200
    
    @task                                   # завантаження кількості магазинів
    def load_stores(): return 15
    
    @task                                   # завантаження товарів
    def load_products(): return 340
    
    @task                                   # підсумкова задача
    def summarize(a,b,c): print(a+b+c)      # підсумовування результатів трьох гілок
    
    s=start(); a=load_sales(); b=load_stores(); c=load_products() # ініціалізація задач
    s >> [a,b,c]                            # паралельне виконання трьох гілок після start
    summarize(a,b,c)                        # зведення результатів усіх гілок

hw_03_parallel()                            # виклик DAG