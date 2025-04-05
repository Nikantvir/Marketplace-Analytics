-- Создание схем для слоев DWH
CREATE SCHEMA IF NOT EXISTS stg_wildberries;
CREATE SCHEMA IF NOT EXISTS stg_ozon;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;

-- Создание расширения для работы с UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Создание таблицы для логирования загрузок в stg слой
CREATE TABLE IF NOT EXISTS stg_wildberries.load_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_type VARCHAR(100) NOT NULL,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    rows_loaded INTEGER,
    file_path VARCHAR(500),
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_ozon.load_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_type VARCHAR(100) NOT NULL,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    rows_loaded INTEGER,
    file_path VARCHAR(500),
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание пользователя для приложения
CREATE USER app_user WITH PASSWORD 'app_password';
GRANT USAGE ON SCHEMA stg_wildberries, stg_ozon, dds, cdm TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg_wildberries, stg_ozon, dds, cdm TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stg_wildberries, stg_ozon, dds, cdm TO app_user;