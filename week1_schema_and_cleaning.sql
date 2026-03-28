-- ============================================================
-- CONSUMER360 | WEEK 1 FINAL WORKING VERSION
-- ============================================================

-- =========================
-- DROP OLD TABLES
-- =========================
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_region CASCADE;
DROP TABLE IF EXISTS stg_raw_transactions;

-- =========================
-- DIM TABLES
-- =========================

CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50) UNIQUE
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_key VARCHAR(50),
    product_name TEXT
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE
);

CREATE TABLE dim_region (
    region_id SERIAL PRIMARY KEY,
    region_name TEXT
);

INSERT INTO dim_region(region_name)
SELECT DISTINCT raw_region FROM stg_raw_transactions;

-- =========================
-- FACT TABLE
-- =========================

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_id INT,
    product_id INT,
    date_id INT,
    region_id INT,
    quantity INT,
    unit_price NUMERIC,
    revenue NUMERIC,
    transaction_date DATE
);

-- =========================
-- STAGING TABLE (FIXED)
-- =========================

CREATE TABLE stg_raw_transactions (
    raw_order_id TEXT,
    raw_product_id TEXT,
    raw_product_name TEXT,
    raw_quantity TEXT,
    raw_transaction_date TEXT,
    raw_unit_price TEXT,
    raw_customer_id TEXT,
    raw_region TEXT
);

-- =========================
-- LOAD CSV (IMPORTANT)
-- =========================

COPY stg_raw_transactions
FROM 'C:/Users/uzmat/Desktop/consumer360/online_retail.csv.csv'
DELIMITER ','
CSV HEADER;

-- =========================
-- CLEAN + INSERT DATA
-- =========================

-- Insert customers
INSERT INTO dim_customer(customer_key)
SELECT DISTINCT raw_customer_id
FROM stg_raw_transactions
WHERE raw_customer_id IS NOT NULL;

-- Insert products
INSERT INTO dim_product(product_key, product_name)
SELECT DISTINCT raw_product_id, raw_product_name
FROM stg_raw_transactions;

-- Insert dates
INSERT INTO dim_date(full_date)
SELECT DISTINCT TO_DATE(raw_transaction_date, 'MM/DD/YYYY')
FROM stg_raw_transactions;

-- Insert regions
INSERT INTO dim_region(region_name)
SELECT DISTINCT raw_region
FROM stg_raw_transactions;

-- =========================
-- INSERT INTO FACT TABLE
-- =========================

INSERT INTO fact_sales (
    order_id,
    customer_id,
    product_id,
    date_id,
    region_id,
    quantity,
    unit_price,
    revenue,
    transaction_date
)
SELECT
    s.raw_order_id,
    c.customer_id,
    p.product_id,
    d.date_id,
    r.region_id,
    CAST(s.raw_quantity AS INT),
    CAST(s.raw_unit_price AS NUMERIC),
    CAST(s.raw_quantity AS INT) * CAST(s.raw_unit_price AS NUMERIC),
    TO_DATE(s.raw_transaction_date, 'MM/DD/YYYY')
FROM stg_raw_transactions s
JOIN dim_customer c ON s.raw_customer_id = c.customer_key
JOIN dim_product p ON s.raw_product_id = p.product_key
JOIN dim_date d ON TO_DATE(s.raw_transaction_date, 'MM/DD/YYYY') = d.full_date
JOIN dim_region r ON s.raw_region = r.region_name
WHERE s.raw_customer_id IS NOT NULL;

-- =========================
-- FINAL VIEW (FOR WEEK 2)
-- =========================

CREATE OR REPLACE VIEW vw_customer_purchase_summary AS
SELECT
    c.customer_id,
    c.customer_key,
    MAX(f.transaction_date) AS last_purchase_date,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.revenue) AS total_revenue,
    AVG(f.revenue) AS avg_order_value,
    MIN(f.transaction_date) AS first_purchase_date
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_key;

-- =========================
-- CHECK DATA
-- =========================

SELECT * FROM vw_customer_purchase_summary LIMIT 10;