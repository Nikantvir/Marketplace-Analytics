CREATE_TABLES_QUERY = """
CREATE SCHEMA IF NOT EXISTS stg;

-- Товары
CREATE TABLE IF NOT EXISTS stg.ozon_products (
    id SERIAL PRIMARY KEY,
    product_id BIGINT UNIQUE,
    offer_id TEXT,
    name TEXT NOT NULL,
    price DECIMAL(15,2),
    old_price DECIMAL(15,2),
    attributes JSONB,
    last_updated TIMESTAMP,
    data_actual_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_products_offer_id ON stg.ozon_products (offer_id);
CREATE INDEX IF NOT EXISTS idx_products_last_updated ON stg.ozon_products (last_updated);

-- Денежные потоки
CREATE TABLE IF NOT EXISTS stg.ozon_cash_flow (
    id SERIAL PRIMARY KEY,
    period_start DATE UNIQUE,
    period_end DATE,
    revenue DECIMAL(18,2),
    expenses DECIMAL(18,2),
    data_actual_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cash_flow_period ON stg.ozon_cash_flow (period_start);

-- Взаиморасчёты
CREATE TABLE IF NOT EXISTS stg.ozon_mutual_settlements (
    id SERIAL PRIMARY KEY,
    report_code TEXT,
    date DATE,
    amount DECIMAL(18,2),
    description TEXT,
    data_actual_date TIMESTAMP,
    UNIQUE (report_code, date)
);
CREATE INDEX IF NOT EXISTS idx_mutual_settlements_date ON stg.ozon_mutual_settlements (date);

CREATE TABLE IF NOT EXISTS stg.ozon_transactions (
    id SERIAL PRIMARY KEY,
    operation_id TEXT UNIQUE,
    operation_date TIMESTAMP,
    amount DECIMAL(15,2),
    commission_amount DECIMAL(15,2),
    data_actual_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON stg.ozon_transactions (operation_date);

CREATE TABLE IF NOT EXISTS stg.ozon_analytics_data (
    id SERIAL PRIMARY KEY,
    dimension TEXT,
    metric DECIMAL(18,2),
    date DATE,
    data_actual_date TIMESTAMP,
    UNIQUE (dimension, date)
);
CREATE INDEX IF NOT EXISTS idx_analytics_date ON stg.ozon_analytics_data (date);

CREATE TABLE IF NOT EXISTS stg.ozon_actions (
    id SERIAL PRIMARY KEY,
    action_id BIGINT UNIQUE,
    title TEXT,
    action_type TEXT,
    date_start DATE,
    date_end DATE,
    data_actual_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_actions_dates ON stg.ozon_actions (date_start, date_end);

CREATE TABLE IF NOT EXISTS stg.ozon_action_products (
    id SERIAL PRIMARY KEY,
    action_id BIGINT REFERENCES stg.ozon_actions(action_id),
    product_id BIGINT,
    date_added TIMESTAMP,
    data_actual_date TIMESTAMP,
    UNIQUE (action_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_action_products ON stg.ozon_action_products (action_id);

CREATE TABLE IF NOT EXISTS stg.ozon_fbo_postings (
    id SERIAL PRIMARY KEY,
    posting_number TEXT UNIQUE,
    created_at TIMESTAMP,
    status TEXT,
    data_actual_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fbo_postings_status ON stg.ozon_fbo_postings (status);

CREATE TABLE IF NOT EXISTS stg.ozon_supply_timeslots (
    id SERIAL PRIMARY KEY,
    supply_order_id BIGINT,
    timeslot_start TIMESTAMP,
    timeslot_end TIMESTAMP,
    data_actual_date TIMESTAMP,
    UNIQUE (supply_order_id, timeslot_start)
);
CREATE INDEX IF NOT EXISTS idx_timeslots_range ON stg.ozon_supply_timeslots (timeslot_start, timeslot_end);

CREATE TABLE IF NOT EXISTS stg.ozon_warehouse_load (
    id SERIAL PRIMARY KEY,
    warehouse_id BIGINT,
    load_factor DECIMAL(5,2),
    date DATE,
    data_actual_date TIMESTAMP,
    UNIQUE (warehouse_id, date)
);
CREATE INDEX IF NOT EXISTS idx_warehouse_load_date ON stg.ozon_warehouse_load (date);

CREATE TABLE IF NOT EXISTS stg.ozon_available_timeslots (
    id SERIAL PRIMARY KEY,
    warehouse_id BIGINT,
    timeslot_start TIMESTAMP,
    timeslot_end TIMESTAMP,
    data_actual_date TIMESTAMP,
    UNIQUE (warehouse_id, timeslot_start)
);
CREATE INDEX IF NOT EXISTS idx_available_timeslots ON stg.ozon_available_timeslots (timeslot_start);
"""