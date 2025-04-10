from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from plugins.ozon.api_client import fetch_data
from plugins.ozon.data_processor import process_and_save
from plugins.ozon.sql_queries import CREATE_TABLES_QUERY

DATA_PATH = "/opt/airflow/data/raw/ozon"
API_LIMIT = 1000
NON_PREMIUM_MAX_DAYS = 90

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

dag = DAG(
    'ozon_full_loader_v2',
    default_args=default_args,
    schedule_interval={
        'transactions': '*/15 * * * *',
        'orders': '*/30 * * * *',
        'analytics_data': '0 4 * * *',
        'warehouse_metrics': '0 6 * * *'
    },
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg'],
)

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql=CREATE_TABLES_QUERY,
    dag=dag,
)

def build_load_task(report_type, endpoint, unique_keys, date_field, is_historical=False):
    def _load_task(**context):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=NON_PREMIUM_MAX_DAYS) if is_historical else end_date - timedelta(days=1)
        
        data = fetch_data(
            endpoint=endpoint,
            date_from=start_date.strftime('%Y-%m-%d'),
            date_to=end_date.strftime('%Y-%m-%d'),
            limit=API_LIMIT
        )
        
        if data:
            process_and_save(
                data=data,
                report_type=report_type,
                unique_keys=unique_keys,
                date_field=date_field,
                data_path=DATA_PATH,
                postgres_conn_id='postgres_default'
            )
    
    return PythonOperator(
        task_id=f'load_{report_type}',
        python_callable=_load_task,
        provide_context=True,
        dag=dag,
    )

tasks_config = [
    # Основные данные
    {
        'report_type': 'transactions',
        'endpoint': '/v3/finance/transaction/list',
        'unique_keys': ['operation_id', 'operation_date'],
        'date_field': 'operation_date'
    },
    {
        'report_type': 'orders',
        'endpoint': '/v3/posting/fbs/list',
        'unique_keys': ['posting_number', 'status'],
        'date_field': 'in_process_at'
    },
    {
        'report_type': 'products',
        'endpoint': '/v2/product/list',
        'unique_keys': ['product_id', 'offer_id'],
        'date_field': 'last_updated',
        'is_historical': True
    },
    
    # Аналитические отчёты
    {
        'report_type': 'stocks_analytics',
        'endpoint': '/v2/analytics/stock_on_warehouses',
        'unique_keys': ['product_id', 'date'],
        'date_field': 'date'
    },
    {
        'report_type': 'turnover',
        'endpoint': '/v1/analytics/stock_on_warehouses',
        'unique_keys': ['sku', 'date'],
        'date_field': 'date'
    },
    
    # Финансовые отчёты
    {
        'report_type': 'cash_flow',
        'endpoint': '/v1/finance/cash-flow-statement/list',
        'unique_keys': ['period_start'],
        'date_field': 'period_start',
        'is_historical': True
    },
    {
        'report_type': 'mutual_settlements',
        'endpoint': '/v1/finance/mutual-settlement',
        'unique_keys': ['date', 'report_code'],
        'date_field': 'date'
    },
    # Новые задачи
    {
        'report_type': 'analytics_data',
        'endpoint': '/v1/analytics/data',
        'unique_keys': ['dimension', 'date'],
        'date_field': 'date',
        'is_historical': True
    },
    {
        'report_type': 'actions',
        'endpoint': '/v1/actions',
        'unique_keys': ['action_id'],
        'date_field': 'date_start'
    },
    {
        'report_type': 'action_products',
        'endpoint': '/v1/actions/products',
        'unique_keys': ['action_id', 'product_id'],
        'date_field': 'date_added'
    },
    {
        'report_type': 'fbo_postings',
        'endpoint': '/v2/posting/fbo/list',
        'unique_keys': ['posting_number'],
        'date_field': 'created_at'
    },
    {
        'report_type': 'supply_timeslots',
        'endpoint': '/v1/supply-order/timeslot/get',
        'unique_keys': ['supply_order_id', 'timeslot_start'],
        'date_field': 'timeslot_start'
    },
    {
        'report_type': 'warehouse_load',
        'endpoint': '/v1/supplier/available_warehouses',
        'unique_keys': ['warehouse_id', 'date'],
        'date_field': 'date'
    },
    {
        'report_type': 'available_timeslots',
        'endpoint': '/v1/draft/timeslot/info',
        'unique_keys': ['warehouse_id', 'timeslot_start'],
        'date_field': 'timeslot_start'
    },
    {
        'report_type': 'products',
        'endpoint': '/v2/product/list',
        'unique_keys': ['product_id'],
        'date_field': 'last_updated',
        'is_historical': True
    }
]

for config in tasks_config:
    task = build_load_task(**config)
    create_tables >> task