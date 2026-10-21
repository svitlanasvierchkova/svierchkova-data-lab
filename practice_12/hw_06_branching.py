# Завдання 6 — розгалуження
from datetime import datetime               # імпорт datetime
from airflow.sdk import dag, task           # імпорт dag та task

@dag(
    dag_id="hw_06_branch",                  # ідентифікатор DAG
    start_date=datetime(2025,1,1),         # стартова дата
    schedule=None,                          # запуск вручну
    catchup=False,                          # catchup вимкнено
    tags=["homework"]                       # мітка
)
def hw_06_branch():
    @task                                   # задача підрахунку рядків
    def count_rows(): return 4820
    
    @task.branch                            # задача-розгалуження (обирає наступний task_id)
    def choose_path(rows): return "load_big" if rows>=1000 else ("load_small" if rows>0 else "no_data") # логіка вибору гілки
    
    @task                                   # гілка: немає даних
    def no_data(): print('no data')
    
    @task                                   # гілка: малий обсяг
    def load_small(): print('small')
    
    @task                                   # гілка: великий обсяг
    def load_big(): print('big')
    
    b=choose_path(count_rows())             # виклик функції вибору гілки
    b >> [no_data(), load_small(), load_big()] # з'єднання гілки з можливими шляхами виконання

hw_06_branch()                              # виклик DAG