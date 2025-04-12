from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from plugins.ozon.api_client import fetch_data
from plugins.ozon.data_processor import process_and_save
from plugins.ozon.sql_queries import CREATE_TABLES_QUERY

# Общие константы
DATA_PATH = "/opt/airflow/data/raw/ozon"
API_LIMIT = 1000
NON_PREMIUM_MAX_DAYS = 90

# Общие аргументы для всех DAG'ов
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

# Функция для создания задачи загрузки данных
def build_load_task(report_type, endpoint, unique_keys, date_field, dag, is_historical=False):
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

# Создаем таблицы в базе данных - общая задача для всех DAG'ов
def create_tables_task(dag):
    return PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql=CREATE_TABLES_QUERY,
        dag=dag,
    )

# DAG для транзакций (каждые 15 минут)
transactions_dag = DAG(
    'ozon_transactions_loader',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'transactions'],
)

create_tables_task_transactions = create_tables_task(transactions_dag)

# Задача для загрузки транзакций
transactions_task = build_load_task(
    report_type='transactions',
    endpoint='/v3/finance/transaction/list',
    unique_keys=['operation_id', 'operation_date'],
    date_field='operation_date',
    dag=transactions_dag
)

create_tables_task_transactions >> transactions_task

# DAG для заказов (каждые 30 минут)
orders_dag = DAG(
    'ozon_orders_loader',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'orders'],
)

create_tables_task_orders = create_tables_task(orders_dag)

# Задачи для заказов
orders_task = build_load_task(
    report_type='orders',
    endpoint='/v3/posting/fbs/list',
    unique_keys=['posting_number', 'status'],
    date_field='in_process_at',
    dag=orders_dag
)

fbo_postings_task = build_load_task(
    report_type='fbo_postings',
    endpoint='/v2/posting/fbo/list',
    unique_keys=['posting_number'],
    date_field='created_at',
    dag=orders_dag
)

create_tables_task_orders >> orders_task
create_tables_task_orders >> fbo_postings_task

# DAG для аналитических данных (раз в день в 4:00)
analytics_dag = DAG(
    'ozon_analytics_loader',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'analytics'],
)

create_tables_task_analytics = create_tables_task(analytics_dag)

# Задачи для аналитических данных
analytics_data_task = build_load_task(
    report_type='analytics_data',
    endpoint='/v1/analytics/data',
    unique_keys=['dimension', 'date'],
    date_field='date',
    dag=analytics_dag,
    is_historical=True
)

stocks_analytics_task = build_load_task(
    report_type='stocks_analytics',
    endpoint='/v2/analytics/stock_on_warehouses',
    unique_keys=['product_id', 'date'],
    date_field='date',
    dag=analytics_dag
)

turnover_task = build_load_task(
    report_type='turnover',
    endpoint='/v1/analytics/stock_on_warehouses',
    unique_keys=['sku', 'date'],
    date_field='date',
    dag=analytics_dag
)

create_tables_task_analytics >> analytics_data_task
create_tables_task_analytics >> stocks_analytics_task
create_tables_task_analytics >> turnover_task

# DAG для складских метрик (раз в день в 6:00)
warehouse_dag = DAG(
    'ozon_warehouse_loader',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'warehouse'],
)

create_tables_task_warehouse = create_tables_task(warehouse_dag)

# Задачи для складских метрик
warehouse_load_task = build_load_task(
    report_type='warehouse_load',
    endpoint='/v1/supplier/available_warehouses',
    unique_keys=['warehouse_id', 'date'],
    date_field='date',
    dag=warehouse_dag
)

available_timeslots_task = build_load_task(
    report_type='available_timeslots',
    endpoint='/v1/draft/timeslot/info',
    unique_keys=['warehouse_id', 'timeslot_start'],
    date_field='timeslot_start',
    dag=warehouse_dag
)

supply_timeslots_task = build_load_task(
    report_type='supply_timeslots',
    endpoint='/v1/supply-order/timeslot/get',
    unique_keys=['supply_order_id', 'timeslot_start'],
    date_field='timeslot_start',
    dag=warehouse_dag
)

create_tables_task_warehouse >> warehouse_load_task
create_tables_task_warehouse >> available_timeslots_task
create_tables_task_warehouse >> supply_timeslots_task

# DAG для товаров и акций (раз в день в 2:00)
products_dag = DAG(
    'ozon_products_loader',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'products'],
)

create_tables_task_products = create_tables_task(products_dag)

# Задачи для товаров и акций
products_task = build_load_task(
    report_type='products',
    endpoint='/v2/product/list',
    unique_keys=['product_id', 'offer_id'],
    date_field='last_updated',
    dag=products_dag,
    is_historical=True
)

actions_task = build_load_task(
    report_type='actions',
    endpoint='/v1/actions',
    unique_keys=['action_id'],
    date_field='date_start',
    dag=products_dag
)

action_products_task = build_load_task(
    report_type='action_products',
    endpoint='/v1/actions/products',
    unique_keys=['action_id', 'product_id'],
    date_field='date_added',
    dag=products_dag
)

create_tables_task_products >> products_task
create_tables_task_products >> actions_task
create_tables_task_products >> action_products_task

# DAG для финансовых отчетов (раз в день в 1:00)
finance_dag = DAG(
    'ozon_finance_loader',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['ozon', 'stg', 'finance'],
)

create_tables_task_finance = create_tables_task(finance_dag)

# Задачи для финансовых отчетов
cash_flow_task = build_load_task(
    report_type='cash_flow',
    endpoint='/v1/finance/cash-flow-statement/list',
    unique_keys=['period_start'],
    date_field='period_start',
    dag=finance_dag,
    is_historical=True
)

mutual_settlements_task = build_load_task(
    report_type='mutual_settlements',
    endpoint='/v1/finance/mutual-settlement',
    unique_keys=['date', 'report_code'],
    date_field='date',
    dag=finance_dag
)

create_tables_task_finance >> cash_flow_task
create_tables_task_finance >> mutual_settlements_task