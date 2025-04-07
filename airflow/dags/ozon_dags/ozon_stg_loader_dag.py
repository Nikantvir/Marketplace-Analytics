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
DATA_PATH = "/opt/airflow/data/raw/ozon"
FIRST_LOAD_DAYS = 30  # Количество дней для первоначальной загрузки (1 год)
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
    'ozon_stg_loader',
    default_args=default_args,
    description='Load data from Ozon API to STG layer',
    schedule_interval='*/5 * * * *',  # Каждые 5 минут
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg'],
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
    
    # Получаем ключи API из Airflow Variables
    ozon_client_id = Variable.get("ozon_client_id")
    ozon_api_key = Variable.get("ozon_api_key")
    
    # Формируем URL API Ozon для получения данных по транзакциям
    url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
    headers = {
        "Client-Id": ozon_client_id,
        "Api-Key": ozon_api_key,
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
        response = safe_api_request('post', url, headers, json=payload)
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

        # Добавляем поле с датой актуальности данных
        current_timestamp = datetime.now()
        df['data_actual_date'] = current_timestamp

        # Приводим JSON поля к строке
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Создаем временную таблицу
        temp_table = f"temp_ozon_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        pg_hook.run(f"CREATE TEMP TABLE {temp_table} (LIKE stg.ozon_transactions INCLUDING ALL)")

        # Загружаем данные во временную таблицу
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY {temp_table} FROM STDIN WITH CSV HEADER",
                filename=f
            )
        
        # Выполняем вставку с проверкой на полные дубликаты
        pg_hook.run(f"""
            INSERT INTO stg.ozon_transactions
            SELECT * FROM {temp_table} t1
            WHERE NOT EXISTS (
                SELECT 1 FROM stg.ozon_transactions t2
                WHERE t2.operation_id = t1.operation_id
                AND t2.operation_type = t1.operation_type
                AND t2.operation_date = t1.operation_date
                AND t2.amount = t1.amount
                AND t2.commission_amount = t1.commission_amount
                AND t2.quantity = t1.quantity
                -- Другие критические поля для сравнения
            )
        """)
        
        # Удаляем временную таблицу
        pg_hook.run(f"DROP TABLE IF EXISTS {temp_table}")
    
    return f"Loaded {len(all_transactions)} transaction records"

# Функция для загрузки данных о заказах
def load_orders_data(**kwargs):
    ti = kwargs['ti']
    period = ti.xcom_pull(task_ids='get_load_period')
    start_date = period['start_date']
    end_date = period['end_date']
    is_initial_load = period['is_initial_load']
    
    # Получаем ключи API из Airflow Variables
    ozon_client_id = Variable.get("ozon_client_id")
    ozon_api_key = Variable.get("ozon_api_key")
    
    url = "https://api-seller.ozon.ru/v3/posting/fbs/list"
    headers = {
        "Client-Id": ozon_client_id,
        "Api-Key": ozon_api_key,
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
        response = safe_api_request('post', url, headers, json=payload)
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

        # Добавляем поле с датой актуальности данных
        current_timestamp = datetime.now()
        df['data_actual_date'] = current_timestamp

        # Приводим JSON поля к строке
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Создаем временную таблицу
        temp_table = f"temp_ozon_orders_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        pg_hook.run(f"CREATE TEMP TABLE {temp_table} (LIKE stg.ozon_orders INCLUDING ALL)")
        
        # Загружаем данные во временную таблицу
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY {temp_table} FROM STDIN WITH CSV HEADER",
                filename=f
            )
        
        # Выполняем вставку с проверкой на полные дубликаты
        pg_hook.run(f"""
            INSERT INTO stg.ozon_orders
            SELECT * FROM {temp_table} t1
            WHERE NOT EXISTS (
                SELECT 1 FROM stg.ozon_orders t2
                WHERE t2.posting_number = t1.posting_number
                AND t2.status = t1.status
                AND t2.order_number = t1.order_number
                AND t2.products = t1.products
                AND t2.financial_data = t1.financial_data
                -- Другие критические поля для сравнения
            )
        """)
        
        # Удаляем временную таблицу
        pg_hook.run(f"DROP TABLE IF EXISTS {temp_table}")

    return f"Loaded {len(all_orders)} order records"

# Функция для загрузки данных о товарах
def load_products_data(**kwargs):
    # Получаем ключи API из Airflow Variables
    ozon_client_id = Variable.get("ozon_client_id")
    ozon_api_key = Variable.get("ozon_api_key")
    
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": ozon_client_id,
        "Api-Key": ozon_api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "filter": {},
        "limit": 1000,
        "offset": 0
    }
    
    all_products = []
    
    while True:
        response = safe_api_request('post', url, headers, json=payload)
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
        
        try:
            detail_response = safe_api_request('post', detail_url, headers, json=detail_payload)
            detail_data = detail_response.json()
            if "result" in detail_data:
                product_details.append(detail_data["result"])
        except Exception as e:
            print(f"Error fetching details for product {product['product_id']}: {str(e)}")
            continue
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"{DATA_PATH}/products_{timestamp}.csv"
    
    if product_details:
        df = pd.DataFrame(product_details)

        # Добавляем поле с датой актуальности данных
        current_timestamp = datetime.now()
        df['data_actual_date'] = current_timestamp

        # Приводим JSON поля к строке
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Создаем временную таблицу
        temp_table = f"temp_ozon_products_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        pg_hook.run(f"CREATE TEMP TABLE {temp_table} (LIKE stg.ozon_products INCLUDING ALL)")
        
        # Загружаем данные во временную таблицу
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY {temp_table} FROM STDIN WITH CSV HEADER",
                filename=f
            )
        
        # Выполняем вставку с проверкой на полные дубликаты (кроме data_actual_date)
        pg_hook.run(f"""
            INSERT INTO stg.ozon_products
            SELECT * FROM {temp_table} t1
            WHERE NOT EXISTS (
                SELECT 1 FROM stg.ozon_products t2
                WHERE t2.product_id = t1.product_id
                AND t2.name = t1.name
                AND t2.description = t1.description
                AND t2.price = t1.price
                AND t2.old_price = t1.old_price
                AND t2.premium_price = t1.premium_price
                AND t2.attributes = t1.attributes
                -- Другие критические поля для сравнения
            )
        """)
        
        # Удаляем временную таблицу
        pg_hook.run(f"DROP TABLE IF EXISTS {temp_table}")

    return f"Loaded {len(product_details)} product records"

# Функция для загрузки данных о складских остатках
def load_stocks_data(**kwargs):
    # Получаем ключи API из Airflow Variables
    ozon_client_id = Variable.get("ozon_client_id")
    ozon_api_key = Variable.get("ozon_api_key")
    
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    headers = {
        "Client-Id": ozon_client_id,
        "Api-Key": ozon_api_key,
        "Content-Type": "application/json"
    }
    
    response = safe_api_request('post', url, headers, json={})
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

        # Добавляем поле с датой актуальности данных
        current_timestamp = datetime.now()
        df['data_actual_date'] = current_timestamp
        
        # Приводим JSON поля к строке
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], (dict, list)):
                df[col] = df[col].apply(json.dumps)
        
        df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        # Загружаем данные в PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Создаем временную таблицу
        temp_table = f"temp_ozon_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        pg_hook.run(f"CREATE TEMP TABLE {temp_table} (LIKE stg.ozon_stocks INCLUDING ALL)")
        
        # Загружаем данные во временную таблицу
        with open(csv_filename, 'r') as f:
            pg_hook.copy_expert(
                sql=f"COPY {temp_table} FROM STDIN WITH CSV HEADER",
                filename=f
            )
        
        # Выполняем вставку с проверкой на полные дубликаты
        pg_hook.run(f"""
            INSERT INTO stg.ozon_stocks
            SELECT * FROM {temp_table} t1
            WHERE NOT EXISTS (
                SELECT 1 FROM stg.ozon_stocks t2
                WHERE t2.product_id = t1.product_id
                AND t2.warehouse_id = t1.warehouse_id
                AND t2.quantity = t1.quantity
                AND t2.date = t1.date
                -- Другие критические поля для сравнения
            )
        """)
        
        # Удаляем временную таблицу
        pg_hook.run(f"DROP TABLE IF EXISTS {temp_table}")

    return f"Loaded {len(all_stocks)} stock records"

# Определение задач для создания таблиц, если они не существуют
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE SCHEMA IF NOT EXISTS stg;
    
    CREATE TABLE IF NOT EXISTS stg.ozon_transactions (
        id SERIAL PRIMARY KEY,
        operation_id TEXT,
        operation_type TEXT,
        operation_date TIMESTAMP,
        operation_type_name TEXT,
        posting_number TEXT,
        order_number TEXT,
        product_id BIGINT,
        sku BIGINT,
        amount DECIMAL(15,2),
        commission_amount DECIMAL(15,2),
        currency_code TEXT,
        items_total_amount DECIMAL(15,2),
        quantity INT,
        services TEXT,
        additional_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_actual_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.ozon_orders (
        id SERIAL PRIMARY KEY,
        order_id TEXT,
        posting_number TEXT,
        order_number TEXT,
        status TEXT,
        cancel_reason_id INT,
        created_at TIMESTAMP,
        in_process_at TIMESTAMP,
        products TEXT,
        analytics_data TEXT,
        financial_data TEXT,
        additional_data TEXT,
        data_actual_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.ozon_products (
        id SERIAL PRIMARY KEY,
        product_id BIGINT,
        offer_id TEXT,
        name TEXT,
        description TEXT,
        category_id INT,
        price DECIMAL(15,2),
        old_price DECIMAL(15,2),
        premium_price DECIMAL(15,2),
        vat TEXT,
        barcode TEXT,
        images TEXT,
        images360 TEXT,
        pdf_list TEXT,
        attributes TEXT,
        complex_attributes TEXT,
        color_image TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_actual_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS stg.ozon_stocks (
        id SERIAL PRIMARY KEY,
        product_id BIGINT,
        offer_id TEXT,
        sku BIGINT,
        warehouse_id INT,
        warehouse_name TEXT,
        quantity INT,
        date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_actual_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Создаем составные уникальные индексы, включающие дату актуальности данных
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_transactions_composite ON stg.ozon_transactions(operation_id, data_actual_date);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_orders_composite ON stg.ozon_orders(posting_number, status, data_actual_date);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_products_composite ON stg.ozon_products(product_id, price, old_price, premium_price, data_actual_date);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_stocks_composite ON stg.ozon_stocks(product_id, warehouse_id, quantity, date, data_actual_date);
    """,
    dag=dag,
)

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
create_tables >> get_period_task >> [load_transactions_task, load_orders_task, load_products_task, load_stocks_task]