"""
DAG для сбора данных о продажах с Wildberries API
"""
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import json
import uuid
import pandas as pd
import backoff
import requests
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Функция для определения периода первичной загрузки
def get_initial_load_period():
    now = datetime.now()
    start_date = (now - relativedelta(years=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Проверяем, была ли уже выполнена первичная загрузка
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    has_data = pg_hook.get_first("""
        SELECT COUNT(*) FROM stg_wildberries.sales LIMIT 1
    """)[0]
    
    if has_data > 0:
        # Если данные уже есть, берем последние данные из базы и от них +1 день
        last_date = pg_hook.get_first("""
            SELECT MAX(date) FROM stg_wildberries.sales
        """)[0]
        if last_date:
            start_date = last_date + timedelta(days=1)
    
    end_date = now
    return start_date, end_date

def save_to_csv(df, report_type, start_date, end_date):
    """Сохранение DataFrame в CSV файл"""
    # Создаем директорию для хранения файлов, если её нет
    base_dir = "/opt/airflow/data/raw/wildberries"
    Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    # Формируем имя файла с датой и временем
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    
    file_path = f"{base_dir}/{report_type}_{start_date_str}_{end_date_str}_{date_str}.csv"
    
    # Сохраняем в CSV
    df.to_csv(file_path, index=False, encoding='utf-8')
    return file_path

def log_load_result(report_type, start_date, end_date, rows_loaded, file_path, status, error_message=None):
    """Логирование результата загрузки в базу данных"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    query = """
    INSERT INTO stg_wildberries.load_logs
    (id, report_type, start_date, end_date, rows_loaded, file_path, status, error_message, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    pg_hook.run(query, parameters=(
        str(uuid.uuid4()),
        report_type,
        start_date,
        end_date,
        rows_loaded,
        file_path,
        status,
        error_message,
        datetime.now()
    ))

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def fetch_wildberries_sales(**kwargs):
    """Получение данных о продажах с API Wildberries"""
    ti = kwargs['ti']
    api_key = Variable.get("WILDBERRIES_API_KEY")
    
    # Определяем период запроса данных
    if kwargs.get('is_initial_load', True):
        start_date, end_date = get_initial_load_period()
    else:
        #