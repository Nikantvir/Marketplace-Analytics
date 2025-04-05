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
OZON_CLIENT_ID = "{{ var.value.ozon_client_id }}"
OZON_API_KEY = "{{ var.value.ozon_api_key }}"
DATA_PATH = "/opt/airflow/data/raw/ozon"
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
    'ozon_stg_loader',
    default_args=default_args,
    description='Load data from Ozon API to STG layer',
    schedule_interval='*/10 * * * *',  # Каждые 10 минут
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg'],
)

# Функция для определения периода загрузки данных
def get_load_period(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Проверяем, была ли уже загрузка данных по транзакциям
    query = "SELECT COUNT(*) FROM stg.ozon_transactions"
    result = pg_hook.get_first(query)
    
    if result[0] == 0:
        # Первичная загрузка за год
        end_date = datetime.now()
        start_date = end_date - timedelta(days=FIRST_LOAD_DAYS)
        is_initial_load = True
    else:
        # Инкрементальная загрузка
        # Получаем максимальную дату из таблицы
        query = "SELECT MAX(operation_date) FROM stg.ozon_transactions"
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

# Функция для загрузки данных о транзакциях
def load_transactions_data(**kwargs):
    ti = kwargs['ti']
    period = ti.xcom_pull(task_ids='get_load_period')
    start_date = period['start_date']
    end_date = period['end_date']
    is_initial_load = period['is_initial_load']
    
    # Формируем URL API Ozon для получения данных по транзакциям
    url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Преобразуем даты в формат для API Ozon
    start_datetime = f"{start_date}T00:00:00.000Z"
    end_datetime = f"{end_date}T23:59:59.999Z"
    
    payload = {
        "filter": {
            "date": {
                "from": start_datetime,
                "to": end_datetime
            }
        },
        "page": 1,
        "page_size": 1000
    }
    
    all_transactions = []
    
    # Постраничная загрузка данных
    while True:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        
        if "result" not in data or "operations" not in data["result"]:
            break
            
        operations = data["result"]["operations"]
        
        if not operations:
            break
            
        all_transactions.extend(operations)
        
        # Увеличиваем номер страницы
        payload["page"] += 1
        
        # Если это не первичная загрузка и мы достигли конца данных, прерываем цикл
        if not is_initial_load and len(operations) < payload["page_size"]:
            break
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/transactions_{timestamp}.csv"
    
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Используем COPY для быстрой загрузки
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.ozon_transactions FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_transactions)} transaction records"

# Функция для загрузки данных о заказах
def load_orders_data(**kwargs):
    ti = kwargs['ti']
    period = ti.xcom_pull(task_ids='get_load_period')
    start_date = period['start_date']
    end_date = period['end_date']
    is_initial_load = period['is_initial_load']
    
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Преобразуем даты в формат для API Ozon
    start_datetime = f"{start_date}T00:00:00.000Z"
    end_datetime = f"{end_date}T23:59:59.999Z"
    
    payload = {
        "filter": {
            "since": start_datetime,
            "to": end_datetime
        },
        "limit": 1000,
        "offset": 0
    }
    
    all_orders = []
    
    while True:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        
        if "result" not in data or "postings" not in data["result"]:
            break
            
        postings = data["result"]["postings"]
        
        if not postings:
            break
            
        all_orders.extend(postings)
        
        # Увеличиваем смещение для следующей страницы
        payload["offset"] += payload["limit"]
        
        # Если это не первичная загрузка и мы достигли конца данных, прерываем цикл
        if not is_initial_load and len(postings) < payload["limit"]:
            break
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/orders_{timestamp}.csv"
    
    if all_orders:
        df = pd.DataFrame(all_orders)
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.ozon_orders FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_orders)} order records"

# Функция для загрузки данных о товарах
def load_products_data(**kwargs):
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    
    payload = {
        "filter": {},
        "limit": 1000,
        "offset": 0
    }
    
    all_products = []
    
    while True:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        
        if "result" not in data or "items" not in data["result"]:
            break
            
        items = data["result"]["items"]
        
        if not items:
            break
            
        all_products.extend(items)
        
        # Увеличиваем смещение для следующей страницы
        payload["offset"] += payload["limit"]
        
        # Если мы достигли конца данных, прерываем цикл
        if len(items) < payload["limit"]:
            break
    
    # Для каждого продукта получаем детальную информацию
    product_details = []
    for product in all_products:
        detail_url = "https://api-seller.ozon.ru/v2/product/info"
        detail_payload = {
            "product_id": product["product_id"],
            "sku": product["offer_id"]
        }
        
        detail_response = requests.post(detail_url, headers=headers, json=detail_payload)
        
        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            if "result" in detail_data:
                product_details.append(detail_data["result"])
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/products_{timestamp}.csv"
    
    if product_details:
        df = pd.DataFrame(product_details)
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.ozon_products FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(product_details)} product records"

# Функция для загрузки данных о складских остатках
def load_stocks_data(**kwargs):
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    headers = {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json={})
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    
    data = response.json()
    
    all_stocks = []
    if "result" in data and "rows" in data["result"]:
        all_stocks = data["result"]["rows"]
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/stocks_{timestamp}.csv"
    
    if all_stocks:
        df = pd.DataFrame(all_stocks)
        # Добавляем текущую дату к данным
        df['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY stg.ozon_stocks FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_stocks)} stock records"

# Определение задач
get_period_task = PythonOperator(
    task_id='get_load_period',
    python_callable=get_load_period,
    dag=dag,
)

load_transactions_task = PythonOperator(
    task_id='load_transactions_data',
    python_callable=load_transactions_data,
    dag=dag,
)

load_orders_task = PythonOperator(
    task_id='load_orders_data',
    python_callable=load_orders_data,
    dag=dag,
)

load_products_task = PythonOperator(
    task_id='load_products_data',
    python_callable=load_products_data,
    dag=dag,
)

load_stocks_task = PythonOperator(
    task_id='load_stocks_data',
    python_callable=load_stocks_data,
    dag=dag,
)

# Определение последовательности выполнения задач
get_period_task >> [load_transactions_task, load_orders_task, load_products_task, load_stocks_task]