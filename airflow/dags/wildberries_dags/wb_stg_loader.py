
from datetime import datetime, timedelta
import os
import csv
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Константы и конфигурации
WB_API_KEY = "{{ var.value.wb_api_key }}"
DATA_PATH = "/opt/airflow/data/raw/wildberries"
FIRST_LOAD_DAYS = 365  # Количество дней для первоначальной загрузки (1 год)
API_REQUEST_LIMIT = 10  # Минимальный интервал между запросами к API (в секундах)

# Убедимся, что директория для сохранения CSV файлов существует
os.makedirs(DATA_PATH, exist_ok=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wildberries_stg_loader',
    default_args=default_args,
    description='Load data from Wildberries API to STG layer',
    schedule_interval='*/10 * * * *',  # Каждые 10 минут
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['wildberries', 'stg'],
)

# Функция для определения периода загрузки данных
def get_load_period(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Проверяем, была ли уже загрузка данных по продажам
    query = "SELECT COUNT(*) FROM stg.wb_sales"
    result = pg_hook.get_first(query)
    
    if result[0] == 0:
        # Первичная загрузка за год
        end_date = datetime.now()
        start_date = end_date - timedelta(days=FIRST_LOAD_DAYS)
        is_initial_load = True
    else:
        # Инкрементальная загрузка
        # Получаем максимальную дату из таблицы
        query = "SELECT MAX(date) FROM stg.wb_sales"
        max_date = pg_hook.get_first(query)[0]
        
        if max_date:
            start_date = max_date
        else:
            start_date = datetime.now() - timedelta(days=1)
            
        end_date = datetime.now()
        is_initial_load = False
    
    return {
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'is_initial_load': is_initial_load
    }

# Функция для загрузки данных о продажах
def load_sales_data(**kwargs):
    ti = kwargs['ti']
    period = ti.xcom_pull(task_ids='get_load_period')
    start_date = period['start_date']
    end_date = period['end_date']
    is_initial_load = period['is_initial_load']
    
    # Формируем URL API Wildberries для получения данных по продажам
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers = {"Authorization": WB_API_KEY}
    params = {
        "dateFrom": start_date,
        "dateTo": end_date,
        "limit": 1000,  # Максимальное количество записей на страницу
        "offset": 0
    }
    
    all_sales = []
    
    # Постраничная загрузка данных
    while True:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        
        if not data:
            break
            
        all_sales.extend(data)
        
        # Увеличиваем смещение для следующей страницы
        params["offset"] += params["limit"]
        
        # Если это не первичная загрузка, прерываем цикл после первой страницы
        # для экономии запросов к API
        if not is_initial_load and len(data) < params["limit"]:
            break
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/sales_{timestamp}.csv"
    
    if all_sales:
        df = pd.DataFrame(all_sales)
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Используем COPY для быстрой загрузки
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.wb_sales FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_sales)} sales records"

# Функция для загрузки данных о заказах
def load_orders_data(**kwargs):
    ti = kwargs['ti']
    period = ti.xcom_pull(task_ids='get_load_period')
    start_date = period['start_date']
    end_date = period['end_date']
    is_initial_load = period['is_initial_load']
    
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": WB_API_KEY}
    params = {
        "dateFrom": start_date,
        "dateTo": end_date,
        "limit": 1000,
        "offset": 0
    }
    
    all_orders = []
    
    while True:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        
        if not data:
            break
            
        all_orders.extend(data)
        
        params["offset"] += params["limit"]
        
        if not is_initial_load and len(data) < params["limit"]:
            break
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/orders_{timestamp}.csv"
    
    if all_orders:
        df = pd.DataFrame(all_orders)
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.wb_orders FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_orders)} order records"

# Функция для загрузки данных о складских остатках
def load_stocks_data(**kwargs):
    # API Wildberries для получения информации о складских остатках
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    headers = {"Authorization": WB_API_KEY}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    
    data = response.json()
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/stocks_{timestamp}.csv"
    
    if data:
        df = pd.DataFrame(data)
        # Добавляем текущую дату к данным
        df['date'] = datetime.now().strftime('%Y-%m-%d')
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.wb_stocks FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(data)} stock records"

# Определение задач
get_period_task = PythonOperator(
    task_id='get_load_period',
    python_callable=get_load_period,
    dag=dag,
)

load_sales_task = PythonOperator(
    task_id='load_sales_data',
    python_callable=load_sales_data,
    dag=dag,
)

load_orders_task = PythonOperator(
    task_id='load_orders_data',
    python_callable=load_orders_data,
    dag=dag,
)

load_stocks_task = PythonOperator(
    task_id='load_stocks_data',
    python_callable=load_stocks_data,
    dag=dag,
)

# Определение последовательности выполнения задач
get_period_task >> [load_sales_task, load_orders_task, load_stocks_task]
