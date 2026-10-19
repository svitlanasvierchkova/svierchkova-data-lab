# Завдання 3 — паралельні гілки
from datetime import datetime
from airflow.sdk import dag, task
@dag(dag_id="hw_03_parallel", start_date=datetime(2025,1,1), schedule=None, catchup=False, tags=["homework"])
def hw_03_parallel():
    @task
    def start(): print("Старт")
    @task
    def load_sales(): return 1200
    @task
    def load_stores(): return 15
    @task
    def load_products(): return 340
    @task
    def summarize(a,b,c): print(a+b+c)
    s=start(); a=load_sales(); b=load_stores(); c=load_products(); s >> [a,b,c]; summarize(a,b,c)
hw_03_parallel()