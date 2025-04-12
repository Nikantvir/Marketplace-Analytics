import pandas as pd
import hashlib
import json
import os
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

def process_and_save(data, report_type, unique_keys, date_field, data_path, postgres_conn_id):
    df = pd.DataFrame(data)
    
    # Автоматическая обработка временных меток
    if date_field in df.columns:
        df[date_field] = pd.to_datetime(df[date_field])
    
    # Генерация хеша для каждой строки
    df['row_hash'] = df.apply(
        lambda row: hashlib.md5(json.dumps(row.to_dict()).encode()).hexdigest(), 
        axis=1
    )
    
    # Сохранение во временный CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(data_path, f"{report_type}_{timestamp}.csv")
    df.to_csv(csv_path, index=False)
    
    # Загрузка в PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Создание временной таблицы с хешем
            cursor.execute(f"""
                CREATE TEMP TABLE temp_data (
                    LIKE stg.ozon_{report_type} 
                    INCLUDING DEFAULTS
                );
                ALTER TABLE temp_data ADD COLUMN row_hash TEXT;
            """)
            
            # Копирование данных
            with open(csv_path, 'r') as f:
                cursor.copy_expert(f"""
                    COPY temp_data FROM STDIN WITH CSV HEADER
                """, f)
            
            # Удаление дубликатов через хеш
            cursor.execute(f"""
                DELETE FROM temp_data
                WHERE row_hash IN (
                    SELECT row_hash 
                    FROM stg.ozon_{report_type}
                );
            """)
            
            # Вставка новых данных
            cursor.execute(f"""
                INSERT INTO stg.ozon_{report_type}
                SELECT {','.join(df.columns)}
                FROM temp_data
            """)
            
        conn.commit()
    
    os.remove(csv_path)