-- Таблицы для хранения сырых данных Ozon

-- Таблица для хранения информации о товарах (карточки)
CREATE TABLE IF NOT EXISTS stg_ozon.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id BIGINT,
    offer_id VARCHAR(100),
    sku BIGINT,
    name VARCHAR(255),
    description TEXT,
    category_id BIGINT,
    category_name VARCHAR(255),
    brand VARCHAR(255),
    barcode VARCHAR(100),
    height INT,
    depth INT,
    width INT,
    dimension_unit VARCHAR(20),
    weight INT,
    weight_unit VARCHAR(20),
    images JSONB,
    attributes JSONB,
    complex_attributes JSONB,
    color_image JSONB,
    status VARCHAR(50),
    state VARCHAR(50),
    visibility VARCHAR(50),
    price JSONB,
    marketing_price NUMERIC(15, 2),
    min_price NUMERIC(15, 2),
    old_price NUMERIC(15, 2),
    premium_price NUMERIC(15, 2),
    recommended_price NUMERIC(15, 2),
    vat VARCHAR(20),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_products_product_id ON stg_ozon.products(product_id);
CREATE INDEX IF NOT EXISTS idx_ozon_products_sku ON stg_ozon.products(sku);

-- Таблица для хранения данных о транзакциях (продажи, возвраты, комиссии и т.д.)
CREATE TABLE IF NOT EXISTS stg_ozon.transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operation_id VARCHAR(100),
    operation_type VARCHAR(50),
    operation_date TIMESTAMP WITH TIME ZONE,
    posting_number VARCHAR(100),
    transaction_id VARCHAR(100),
    order_id VARCHAR(100),
    service_id VARCHAR(100),
    service_name VARCHAR(255),
    sku BIGINT,
    product_id BIGINT,
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    quantity INT,
    price NUMERIC(15, 2),
    old_price NUMERIC(15, 2),
    total NUMERIC(15, 2),
    delivery_cost NUMERIC(15, 2),
    return_reason VARCHAR(255),
    commission_amount NUMERIC(15, 2),
    commission_percent NUMERIC(10, 2),
    payout NUMERIC(15, 2),
    currency_code VARCHAR(10),
    region VARCHAR(255),
    warehouse_id BIGINT,
    warehouse_name VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_transactions_operation_date ON stg_ozon.transactions(operation_date);
CREATE INDEX IF NOT EXISTS idx_ozon_transactions_product_id ON stg_ozon.transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_ozon_transactions_sku ON stg_ozon.transactions(sku);

-- Таблица для хранения данных об остатках
CREATE TABLE IF NOT EXISTS stg_ozon.stocks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id BIGINT,
    sku BIGINT,
    offer_id VARCHAR(100),
    warehouse_id BIGINT,
    warehouse_name VARCHAR(255),
    present INT,
    reserved INT,
    date_check TIMESTAMP WITH TIME ZONE,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_stocks_product_id ON stg_ozon.stocks(product_id);
CREATE INDEX IF NOT EXISTS idx_ozon_stocks_date_check ON stg_ozon.stocks(date_check);

-- Таблица для хранения данных о заказах
CREATE TABLE IF NOT EXISTS stg_ozon.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id BIGINT,
    order_number VARCHAR(100),
    posting_number VARCHAR(100),
    order_date TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    delivery_method_id BIGINT,
    delivery_method_name VARCHAR(100),
    warehouse_id BIGINT,
    warehouse_name VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255),
    delivery_address TEXT,
    is_premium BOOLEAN,
    payment_type_id BIGINT,
    payment_type_name VARCHAR(100),
    product_id BIGINT,
    sku BIGINT,
    name VARCHAR(255),
    quantity INT,
    offer_id VARCHAR(100),
    price NUMERIC(15, 2),
    customer_price NUMERIC(15, 2),
    commission_amount NUMERIC(15, 2),
    commission_percent NUMERIC(10, 2),
    payout NUMERIC(15, 2),
    in_process_at TIMESTAMP WITH TIME ZONE,
    shipping_provider_id BIGINT,
    shipping_provider VARCHAR(255),
    tracking_number VARCHAR(100),
    shipment_date TIMESTAMP WITH TIME ZONE,
    delivering_date TIMESTAMP WITH TIME ZONE,
    cancellation VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_orders_order_date ON stg_ozon.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_ozon_orders_product_id ON stg_ozon.orders(product_id);

-- Таблица для хранения данных о ценах конкурентов
CREATE TABLE IF NOT EXISTS stg_ozon.price_index (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id BIGINT,
    sku BIGINT,
    offer_id VARCHAR(100),
    price NUMERIC(15, 2),
    min_price NUMERIC(15, 2),
    max_price NUMERIC(15, 2),
    average_price NUMERIC(15, 2),
    median_price NUMERIC(15, 2),
    market_count INT,
    date_check TIMESTAMP WITH TIME ZONE,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_price_index_product_id ON stg_ozon.price_index(product_id);
CREATE INDEX IF NOT EXISTS idx_ozon_price_index_date_check ON stg_ozon.price_index(date_check);

-- Таблица для хранения данных по отчетам о реализации (финансы)
CREATE TABLE IF NOT EXISTS stg_ozon.finance_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_id VARCHAR(100),
    delivery_schema VARCHAR(50),
    period_from TIMESTAMP WITH TIME ZONE,
    period_to TIMESTAMP WITH TIME ZONE,
    product_id BIGINT,
    product_name VARCHAR(255),
    offer_id VARCHAR(100),
    sku BIGINT,
    quantity INT,
    price NUMERIC(15, 2),
    commission_amount NUMERIC(15, 2),
    commission_percent NUMERIC(10, 2),
    sale_amount NUMERIC(15, 2),
    sale_discount NUMERIC(15, 2),
    extra_charge_amount NUMERIC(15, 2),
    return_amount NUMERIC(15, 2),
    processing_and_delivery NUMERIC(15, 2),
    refund_processing_and_delivery NUMERIC(15, 2),
    services_amount NUMERIC(15, 2),
    total_payment_amount NUMERIC(15, 2),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ozon_finance_reports_period ON stg_ozon.finance_reports(period_from, period_to);
CREATE INDEX IF NOT EXISTS idx_ozon_finance_reports_product_id ON stg_ozon.finance_reports(product_id);