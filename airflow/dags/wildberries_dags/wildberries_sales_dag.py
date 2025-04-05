from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
dag = DAG(
    'wildberries_sales_daily',
    default_args=default_args,
    description='Получение данных о продажах с Wildberries API',
    schedule_interval='0 1 * * *',  # Каждый день в 01:00
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def fetch_wildberries_sales(**kwargs):
    """Получение данных о продажах с API Wildberries"""
    api_key = os.getenv('WILDBERRIES_API_KEY')
    
    # Параметры запроса (последние сутки)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%dT00:00:00.000Z')
    today_str = datetime.now().strftime('%Y-%m-%dT00:00:00.000Z')
    
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers = {
        "Authorization": api_key
    }
    params = {
        "dateFrom": yesterday_str,
        "dateTo": today_str
    }
    
    # Выполняем запрос
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"API вернул ошибку: {response.status_code}, {response.text}")
    
    sales_data = response.json()
    
    # Обработка и сохранение данных
    if not sales_data:
        print("Нет данных о продажах за выбранный период")
        return None
    
    # Преобразуем данные в DataFrame
    df = pd.DataFrame(sales_data)
    
    # Сохраняем во временный файл для последующей загрузки
    temp_path = f"/tmp/wb_sales_{kwargs['ds']}.csv"
    df.to_csv(temp_path, index=False)
    
    return temp_path


def load_sales_to_postgres(**kwargs):
    """Загрузка данных о продажах в PostgreSQL"""
    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids='fetch_wildberries_sales')
    
    if not temp_path:
        print("Нет данных для загрузки")
        return
    
    # Загружаем данные из CSV
    df = pd.read_csv(temp_path)
    
    # Преобразуем даты
    df['sale_dt'] = pd.to_datetime(df['date'])
    
    # Создаем подключение к базе данных
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Подготовка данных для загрузки
    for index, row in df.iterrows():
        # Проверяем, существует ли продукт, если нет - добавляем
        cursor.execute(
            """
            INSERT INTO wildberries.products (nm_id, name, brand, supplier_id, category, subject)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nm_id) DO UPDATE 
            SET updated_at = NOW()
            """,
            (row['nmId'], row['subject'], row['brand'], row['supplierArticle'], row['category'], row['subject'])
        )
        
        # Записываем продажу
        cursor.execute(
            """
            INSERT INTO wildberries.sales 
            (sale_id, nm_id, quantity, price_with_disc, price, sale_dt, subject, category, brand)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING
            """,
            (
                row['saleID'], 
                row['nmId'], 
                row['quantity'], 
                row['finishedPrice'], 
                row['forPay'], 
                row['sale_dt'],
                row['subject'],
                row['category'],
                row['brand']
            )
        )
    
    # Фиксируем изменения
    conn.commit()
    cursor.close()
    conn.close()
    
    # Удаляем временный файл
    os.remove(temp_path)


# Определяем задачи
fetch_task = PythonOperator(
    task_id='fetch_wildberries_sales',
    python_callable=fetch_wildberries_sales,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_sales_to_postgres',
    python_callable=load_sales_to_postgres,
    provide_context=True,
    dag=dag,
)

# Определяем порядок выполнения задач
fetch_task >> load_task