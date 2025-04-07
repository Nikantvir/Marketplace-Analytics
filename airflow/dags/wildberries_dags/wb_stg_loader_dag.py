from datetime import datetime, timedelta
import os
import csv
import json
import requests
import pandas as pd
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Константы и конфигурации
DATA_PATH = "/opt/airflow/data/raw/wildberries"
FIRST_LOAD_DAYS = 365  # Количество дней для первоначальной загрузки (1 год)
API_REQUEST_LIMIT = 1  # Минимальный интервал между запросами к API (в секундах)

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
    schedule_interval='*/5 * * * *',  # Каждые 5 минут (как у Ozon для согласованности)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['wildberries', 'stg'],
)

# Функция для безопасного выполнения API запросов с повторными попытками
def safe_api_request(method, url, headers, json=None, params=None, max_retries=3, retry_delay=5):
    retries = 0
    while retries < max_retries:
        try:
            time.sleep(API_REQUEST_LIMIT)  # Делаем паузу между запросами
            if method.lower() == 'get':
                response = requests.get(url, headers=headers, params=params)
            else:  # По умолчанию используем POST
                response = requests.post(url, headers=headers, json=json)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Too Many Requests
                retries += 1
                time.sleep(retry_delay * (2 ** retries))  # Экспоненциальная задержка
                continue
            else:
                raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries >= max_retries:
                raise Exception(f"Failed after {max_retries} retries. Error: {str(e)}")
            time.sleep(retry_delay * (2 ** retries))
    
    raise Exception(f"Failed to make API request after {max_retries} retries")

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
    
    # Получаем ключ API из Airflow Variables
    wb_api_key = Variable.get("wb_api_key")
    
    # Формируем URL API Wildberries для получения данных по продажам
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers = {"Authorization": wb_api_key}
    params = {
        "dateFrom": start_date,
        "dateTo": end_date,
        "limit": 1000,  # Максимальное количество записей на страницу
        "offset": 0
    }
    
    all_sales = []
    
    # Постраничная загрузка данных
    while True:
        response = safe_api_request('get', url, headers, params=params)
        data = response.json()
        
        if not data:
            break
            
        all_sales.extend(data)
        
        # Увеличиваем смещение для следующей страницы
        params["offset"] += params["limit"]
        
        # Если это не первичная загрузка и мы достигли конца данных, прерываем цикл
        if not is_initial_load and len(data) < params["limit"]:
            break
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/sales_{timestamp}.csv"
    
    if all_sales:
        df = pd.DataFrame(all_sales)
        
        # Приводим JSON поля к строке, если они есть
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
                
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Используем COPY для быстрой загрузки
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql="COPY stg.wb_sales FROM STDIN WITH CSV HEADER",
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
    
    # Получаем ключ API из Airflow Variables
    wb_api_key = Variable.get("wb_api_key")
    
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": wb_api_key}
    params = {
        "dateFrom": start_date,
        "dateTo": end_date,
        "limit": 1000,
        "offset": 0
    }
    
    all_orders = []
    
    while True:
        response = safe_api_request('get', url, headers, params=params)
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
        
        # Приводим JSON поля к строке, если они есть
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
                
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql="COPY stg.wb_orders FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(all_orders)} order records"

# Функция для загрузки данных о складских остатках
def load_stocks_data(**kwargs):
    # Получаем ключ API из Airflow Variables
    wb_api_key = Variable.get("wb_api_key")
    
    # API Wildberries для получения информации о складских остатках
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    headers = {"Authorization": wb_api_key}
    
    response = safe_api_request('get', url, headers)
    data = response.json()
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/stocks_{timestamp}.csv"
    
    if data:
        df = pd.DataFrame(data)
        # Добавляем текущую дату к данным
        df['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Приводим JSON поля к строке, если они есть
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
                
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql="COPY stg.wb_stocks FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(data)} stock records"

# Функция для загрузки данных о товарах
def load_products_data(**kwargs):
    # Получаем ключ API из Airflow Variables
    wb_api_key = Variable.get("wb_api_key")
    
    # API Wildberries для получения информации о товарах
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/cards"
    headers = {"Authorization": wb_api_key}
    
    response = safe_api_request('get', url, headers)
    data = response.json()
    
    # Сохраняем данные в CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/products_{timestamp}.csv"
    
    if data:
        df = pd.DataFrame(data)
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
                
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql="COPY stg.wb_products FROM STDIN WITH CSV HEADER",
                filename=f
            )
    
    return f"Loaded {len(data)} product records"

# Определение задач для создания таблиц, если они не существуют
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE SCHEMA IF NOT EXISTS stg;
    
    CREATE TABLE IF NOT EXISTS stg.wb_sales (
        id SERIAL PRIMARY KEY,
        srid BIGINT,
        date TIMESTAMP,
        lastChangeDate TIMESTAMP,
        supplierArticle TEXT,
        techSize TEXT,
        barcode TEXT,
        totalPrice DECIMAL(15,2),
        discountPercent INT,
        isSupply BOOLEAN,
        isRealization BOOLEAN,
        promoCodeDiscount DECIMAL(15,2),
        warehouseName TEXT,
        countryName TEXT,
        oblastOkrugName TEXT,
        regionName TEXT,
        incomeID BIGINT,
        saleID TEXT,
        odid BIGINT,
        spp DECIMAL(15,2),
        forPay DECIMAL(15,2),
        finishedPrice DECIMAL(15,2),
        priceWithDisc DECIMAL(15,2),
        nmId BIGINT,
        subject TEXT,
        category TEXT,
        brand TEXT,
        is_storno INT,
        sticker TEXT,
        additional_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.wb_orders (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        lastChangeDate TIMESTAMP,
        supplierArticle TEXT,
        techSize TEXT,
        barcode TEXT,
        totalPrice DECIMAL(15,2),
        discountPercent INT,
        warehouseName TEXT,
        oblast TEXT,
        incomeID BIGINT,
        odid BIGINT,
        nmId BIGINT,
        subject TEXT,
        category TEXT,
        brand TEXT,
        is_cancel BOOLEAN,
        cancel_dt TIMESTAMP,
        sticker TEXT,
        additional_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.wb_stocks (
        id SERIAL PRIMARY KEY,
        date DATE,
        lastChangeDate TIMESTAMP,
        supplierArticle TEXT,
        techSize TEXT,
        barcode TEXT,
        quantity INT,
        isSupply BOOLEAN,
        isRealization BOOLEAN,
        quantityFull INT,
        quantityNotInOrders INT,
        warehouseName TEXT,
        inWayToClient INT,
        inWayFromClient INT,
        nmId BIGINT,
        subject TEXT,
        category TEXT,
        daysOnSite INT,
        brand TEXT,
        SCCode TEXT,
        Price DECIMAL(15,2),
        Discount INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.wb_products (
        id SERIAL PRIMARY KEY,
        imtID BIGINT,
        nmID BIGINT,
        vendorCode TEXT,
        name TEXT,
        brand TEXT,
        barcodes TEXT,
        sizes TEXT,
        colors TEXT,
        mediaFiles TEXT,
        subjects TEXT,
        supplierVendorCode TEXT,
        updated_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

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

load_products_task = PythonOperator(
    task_id='load_products_data',
    python_callable=load_products_data,
    dag=dag,
)

# Определение последовательности выполнения задач
create_tables >> get_period_task >> [load_sales_task, load_orders_task, load_stocks_task, load_products_task]