-- Таблицы для хранения сырых данных Wildberries

-- Таблица для хранения информации о товарах (карточки)
CREATE TABLE IF NOT EXISTS stg_wildberries.cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    imtId BIGINT,
    nmID BIGINT,
    vendorCode VARCHAR(100),
    brand VARCHAR(255),
    subject VARCHAR(255),
    subjectID INT,
    category VARCHAR(255),
    categoryID INT,
    description TEXT,
    characteristics JSONB,
    sizes JSONB,
    colors JSONB,
    photos JSONB,
    metadata JSONB,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Таблица для хранения данных о продажах
CREATE TABLE IF NOT EXISTS stg_wildberries.sales (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saleID VARCHAR(100),
    gNumber VARCHAR(50),
    sticker VARCHAR(100),
    rrdID INT,
    subject VARCHAR(255),
    supplierArticle VARCHAR(100),
    organizationName VARCHAR(255),
    date TIMESTAMP WITH TIME ZONE,
    lastChangeDate TIMESTAMP WITH TIME ZONE,
    barcode VARCHAR(100),
    totalPrice NUMERIC(15, 2),
    finishedPrice NUMERIC(15, 2),
    forPay NUMERIC(15, 2),
    nmId BIGINT,
    brand VARCHAR(255),
    techSize VARCHAR(50),
    quantity INT,
    warehouseName VARCHAR(255),
    oblast VARCHAR(255),
    incomeID BIGINT,
    odid BIGINT,
    spp NUMERIC(10, 2),
    srid VARCHAR(100),
    discountPercent INT,
    country VARCHAR(100),
    regionName VARCHAR(255),
    oblastOkrugName VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_sales_nmid ON stg_wildberries.sales(nmId);
CREATE INDEX IF NOT EXISTS idx_wb_sales_date ON stg_wildberries.sales(date);

-- Таблица для хранения данных об остатках
CREATE TABLE IF NOT EXISTS stg_wildberries.stocks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date TIMESTAMP WITH TIME ZONE,
    lastChangeDate TIMESTAMP WITH TIME ZONE,
    warehouseName VARCHAR(255),
    supplierArticle VARCHAR(100),
    barcode VARCHAR(100),
    quantity INT,
    quantityFull INT,
    quantityNotInOrders INT,
    inWayToClient INT,
    inWayFromClient INT,
    nmId BIGINT,
    subject VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    techSize VARCHAR(50),
    price NUMERIC(15, 2),
    discount INT,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_nmid ON stg_wildberries.stocks(nmId);
CREATE INDEX IF NOT EXISTS idx_wb_stocks_date ON stg_wildberries.stocks(date);

-- Таблица для хранения данных о заказах
CREATE TABLE IF NOT EXISTS stg_wildberries.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    orderID VARCHAR(100),
    date TIMESTAMP WITH TIME ZONE,
    lastChangeDate TIMESTAMP WITH TIME ZONE,
    supplierArticle VARCHAR(100),
    barcode VARCHAR(100),
    totalPrice NUMERIC(15, 2),
    nmId BIGINT,
    brand VARCHAR(255),
    subject VARCHAR(255),
    techSize VARCHAR(50),
    quantity INT,
    warehouseName VARCHAR(255),
    oblast VARCHAR(255),
    incomeID BIGINT,
    odid BIGINT,
    status INT,
    sticker VARCHAR(100),
    srid VARCHAR(100),
    cancel_dt TIMESTAMP WITH TIME ZONE,
    gNumber VARCHAR(50),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_orders_nmid ON stg_wildberries.orders(nmId);
CREATE INDEX IF NOT EXISTS idx_wb_orders_date ON stg_wildberries.orders(date);

-- Таблица для хранения данных о поставках
CREATE TABLE IF NOT EXISTS stg_wildberries.supplies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incomeId BIGINT,
    number VARCHAR(100),
    date TIMESTAMP WITH TIME ZONE,
    lastChangeDate TIMESTAMP WITH TIME ZONE,
    supplierArticle VARCHAR(100),
    techSize VARCHAR(50),
    barcode VARCHAR(100),
    quantity INT,
    totalPrice NUMERIC(15, 2),
    dateClose TIMESTAMP WITH TIME ZONE,
    warehouseName VARCHAR(255),
    nmId BIGINT,
    status VARCHAR(100),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_supplies_nmid ON stg_wildberries.supplies(nmId);
CREATE INDEX IF NOT EXISTS idx_wb_supplies_date ON stg_wildberries.supplies(date);

-- Таблица для хранения данных о финансах (отчеты о продажах)
CREATE TABLE IF NOT EXISTS stg_wildberries.reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    realizationreportId BIGINT,
    suppliercontractCode VARCHAR(100),
    rrdId BIGINT,
    rr_dt TIMESTAMP WITH TIME ZONE,
    rr_office_name VARCHAR(255),
    gi_id BIGINT,
    subject_name VARCHAR(255),
    nm_id BIGINT,
    brand_name VARCHAR(255),
    sa_name VARCHAR(255),
    ts_name VARCHAR(50),
    barcode VARCHAR(100),
    doc_type_name VARCHAR(100),
    quantity INT,
    retail_price NUMERIC(15, 2),
    retail_amount NUMERIC(15, 2),
    sale_percent NUMERIC(10, 2),
    commission_percent NUMERIC(10, 2),
    customer_reward NUMERIC(15, 2),
    supplier_reward NUMERIC(15, 2),
    supplier_reward_nds NUMERIC(15, 2),
    delivery_rub NUMERIC(15, 2),
    return_amount NUMERIC(15, 2),
    delivery_amount NUMERIC(15, 2),
    gi_box_type_name VARCHAR(255),
    product_discount_for_report NUMERIC(15, 2),
    supplier_promo NUMERIC(15, 2),
    rid BIGINT,
    ppvz_spp_prc NUMERIC(10, 2),
    ppvz_kvw_prc_base NUMERIC(10, 2),
    ppvz_kvw_prc NUMERIC(10, 2),
    ppvz_sales_commission NUMERIC(15, 2),
    ppvz_reward NUMERIC(15, 2),
    ppvz_office_id BIGINT,
    ppvz_office_name VARCHAR(255),
    ppvz_supplier_id BIGINT,
    ppvz_supplier_name VARCHAR(255),
    ppvz_inn VARCHAR(20),
    site_country VARCHAR(100),
    penalty NUMERIC(15, 2),
    additional_payment NUMERIC(15, 2),
    srid VARCHAR(100),
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_reports_nm_id ON stg_wildberries.reports(nm_id);
CREATE INDEX IF NOT EXISTS idx_wb_reports_rr_dt ON stg_wildberries.reports(rr_dt);