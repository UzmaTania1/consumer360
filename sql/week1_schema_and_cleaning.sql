-- ============================================================
-- CONSUMER360 | PROJECT 1: RETAIL ANALYTICS
-- WEEK 1: STAR SCHEMA DESIGN + DATA CLEANING PIPELINE
-- Zaalima Development Pvt. Ltd.
-- ============================================================

-- ============================================================
-- PART 1: CREATE DIMENSION AND FACT TABLES (STAR SCHEMA)
-- ============================================================

-- Drop tables if they exist (clean slate)
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_region CASCADE;

-- -----------------------------------------------
-- DIM_CUSTOMER: Customer dimension table
-- -----------------------------------------------
CREATE TABLE dim_customer (
    customer_id     SERIAL PRIMARY KEY,
    customer_key    VARCHAR(50) UNIQUE NOT NULL,       -- Business key from source
    full_name       VARCHAR(150),
    email           VARCHAR(200),
    phone           VARCHAR(30),
    signup_date     DATE,
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    region_id       INT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------
-- DIM_PRODUCT: Product dimension table
-- -----------------------------------------------
CREATE TABLE dim_product (
    product_id      SERIAL PRIMARY KEY,
    product_key     VARCHAR(50) UNIQUE NOT NULL,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100),
    unit_cost       NUMERIC(10, 2),
    unit_price      NUMERIC(10, 2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------
-- DIM_DATE: Date dimension table (calendar spine)
-- -----------------------------------------------
CREATE TABLE dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE NOT NULL,
    day_of_week     INT,        -- 1=Monday ... 7=Sunday
    day_name        VARCHAR(15),
    week_number     INT,
    month_num       INT,
    month_name      VARCHAR(15),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- Populate DIM_DATE for 2022-2025 range
INSERT INTO dim_date (
    full_date, day_of_week, day_name, week_number,
    month_num, month_name, quarter, year, is_weekend
)
SELECT
    d::DATE,
    EXTRACT(ISODOW FROM d)::INT,
    TO_CHAR(d, 'Day'),
    EXTRACT(WEEK FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(YEAR FROM d)::INT,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END
FROM GENERATE_SERIES('2022-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) d;

-- -----------------------------------------------
-- DIM_REGION: Geographic region dimension
-- -----------------------------------------------
CREATE TABLE dim_region (
    region_id       SERIAL PRIMARY KEY,
    region_name     VARCHAR(100) NOT NULL,
    country         VARCHAR(100),
    continent       VARCHAR(50)
);

INSERT INTO dim_region (region_name, country, continent) VALUES
('North',  'India', 'Asia'),
('South',  'India', 'Asia'),
('East',   'India', 'Asia'),
('West',   'India', 'Asia'),
('Central','India', 'Asia');

-- -----------------------------------------------
-- FACT_SALES: Central fact table
-- -----------------------------------------------
CREATE TABLE fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    order_id        VARCHAR(80) NOT NULL,
    customer_id     INT NOT NULL REFERENCES dim_customer(customer_id),
    product_id      INT NOT NULL REFERENCES dim_product(product_id),
    date_id         INT NOT NULL REFERENCES dim_date(date_id),
    region_id       INT REFERENCES dim_region(region_id),
    quantity        INT NOT NULL DEFAULT 1,
    unit_price      NUMERIC(10, 2) NOT NULL,
    discount_pct    NUMERIC(5, 2) DEFAULT 0.00,
    revenue         NUMERIC(12, 2) GENERATED ALWAYS AS
                        (ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)) STORED,
    cost            NUMERIC(12, 2),
    profit          NUMERIC(12, 2) GENERATED ALWAYS AS
                        (ROUND(quantity * unit_price * (1 - discount_pct / 100) - cost, 2)) STORED,
    transaction_date DATE NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- PART 2: INDEXES FOR SUB-2-SECOND QUERY PERFORMANCE
-- (Critical Review Point: Week 1)
-- ============================================================
CREATE INDEX idx_fact_sales_customer   ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product    ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date       ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_region     ON fact_sales(region_id);
CREATE INDEX idx_fact_sales_order      ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_txn_date   ON fact_sales(transaction_date DESC);
CREATE INDEX idx_dim_customer_key      ON dim_customer(customer_key);
CREATE INDEX idx_dim_product_key       ON dim_product(product_key);

-- ============================================================
-- PART 3: RAW STAGING TABLE (holds dirty incoming data)
-- ============================================================
DROP TABLE IF EXISTS stg_raw_transactions;

CREATE TABLE stg_raw_transactions (
    raw_id              SERIAL PRIMARY KEY,
    raw_order_id        TEXT,
    raw_customer_id     TEXT,
    raw_customer_name   TEXT,
    raw_email           TEXT,
    raw_product_id      TEXT,
    raw_product_name    TEXT,
    raw_category        TEXT,
    raw_quantity        TEXT,      -- may contain nulls, strings, negatives
    raw_unit_price      TEXT,      -- may contain currency symbols: "$12.50"
    raw_discount        TEXT,      -- may be "10%", "0.1", or NULL
    raw_transaction_date TEXT,     -- may be "12/31/2024", "2024-12-31", etc.
    raw_region          TEXT,
    load_timestamp      TIMESTAMP DEFAULT NOW(),
    is_processed        BOOLEAN DEFAULT FALSE,
    error_flag          TEXT       -- captures reason if row is rejected
);

-- ============================================================
-- PART 4: DATA CLEANING STORED PROCEDURE
-- Purpose: Standardize NULLs, fix date formats, validate
--          data types, then insert into fact/dimension tables.
-- ============================================================
CREATE OR REPLACE PROCEDURE sp_clean_and_load_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    rec             stg_raw_transactions%ROWTYPE;
    v_customer_id   INT;
    v_product_id    INT;
    v_date_id       INT;
    v_region_id     INT;
    v_quantity      INT;
    v_unit_price    NUMERIC(10,2);
    v_discount      NUMERIC(5,2);
    v_clean_date    DATE;
    v_cost          NUMERIC(12,2);
    v_error         TEXT;
BEGIN
    FOR rec IN
        SELECT * FROM stg_raw_transactions WHERE is_processed = FALSE
    LOOP
        v_error := NULL;

        -- ---- VALIDATE & CLEAN QUANTITY ----
        BEGIN
            v_quantity := TRIM(rec.raw_quantity)::INT;
            IF v_quantity <= 0 THEN
                v_error := 'Invalid quantity: ' || rec.raw_quantity;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_error := 'Non-numeric quantity: ' || COALESCE(rec.raw_quantity, 'NULL');
        END;

        -- ---- VALIDATE & CLEAN UNIT PRICE ----
        BEGIN
            -- Strip currency symbols, commas, spaces
            v_unit_price := REGEXP_REPLACE(
                COALESCE(rec.raw_unit_price, '0'), '[^0-9.]', '', 'g'
            )::NUMERIC(10,2);
            IF v_unit_price <= 0 THEN
                v_error := COALESCE(v_error, '') || ' | Zero/negative price';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_error := COALESCE(v_error, '') || ' | Bad price: ' || rec.raw_unit_price;
        END;

        -- ---- VALIDATE & CLEAN DISCOUNT ----
        BEGIN
            IF rec.raw_discount IS NULL OR TRIM(rec.raw_discount) = '' THEN
                v_discount := 0.00;
            ELSIF rec.raw_discount LIKE '%\%%' THEN
                -- "10%" -> 10.00
                v_discount := REPLACE(rec.raw_discount, '%', '')::NUMERIC(5,2);
            ELSIF rec.raw_discount::NUMERIC < 1 THEN
                -- "0.10" -> 10.00 (decimal format)
                v_discount := (rec.raw_discount::NUMERIC * 100)::NUMERIC(5,2);
            ELSE
                v_discount := rec.raw_discount::NUMERIC(5,2);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_discount := 0.00;
        END;

        -- ---- VALIDATE & CLEAN TRANSACTION DATE ----
        BEGIN
            -- Handle: YYYY-MM-DD, MM/DD/YYYY, DD-MM-YYYY
            v_clean_date := CASE
                WHEN rec.raw_transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN rec.raw_transaction_date::DATE
                WHEN rec.raw_transaction_date ~ '^\d{2}/\d{2}/\d{4}$'
                    THEN TO_DATE(rec.raw_transaction_date, 'MM/DD/YYYY')
                WHEN rec.raw_transaction_date ~ '^\d{2}-\d{2}-\d{4}$'
                    THEN TO_DATE(rec.raw_transaction_date, 'DD-MM-YYYY')
                ELSE NULL
            END;
            IF v_clean_date IS NULL THEN
                v_error := COALESCE(v_error, '') || ' | Unparseable date: ' || rec.raw_transaction_date;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_error := COALESCE(v_error, '') || ' | Date error';
        END;

        -- ---- FLAG & SKIP BAD ROWS ----
        IF v_error IS NOT NULL THEN
            UPDATE stg_raw_transactions
            SET is_processed = TRUE, error_flag = v_error
            WHERE raw_id = rec.raw_id;
            CONTINUE;
        END IF;

        -- ---- UPSERT DIM_CUSTOMER ----
        INSERT INTO dim_customer (customer_key, full_name, email, region_id)
        VALUES (
            COALESCE(NULLIF(TRIM(rec.raw_customer_id), ''), 'UNKNOWN_' || rec.raw_id),
            INITCAP(TRIM(rec.raw_customer_name)),
            LOWER(TRIM(rec.raw_email)),
            NULL
        )
        ON CONFLICT (customer_key) DO UPDATE
            SET full_name  = EXCLUDED.full_name,
                updated_at = NOW()
        RETURNING customer_id INTO v_customer_id;

        -- ---- UPSERT DIM_PRODUCT ----
        INSERT INTO dim_product (product_key, product_name, category)
        VALUES (
            COALESCE(NULLIF(TRIM(rec.raw_product_id), ''), 'PROD_' || rec.raw_id),
            INITCAP(TRIM(rec.raw_product_name)),
            INITCAP(TRIM(rec.raw_category))
        )
        ON CONFLICT (product_key) DO UPDATE
            SET product_name = EXCLUDED.product_name
        RETURNING product_id INTO v_product_id;

        -- ---- LOOKUP DATE ID ----
        SELECT date_id INTO v_date_id
        FROM dim_date WHERE full_date = v_clean_date;

        -- ---- LOOKUP REGION ID ----
        SELECT region_id INTO v_region_id
        FROM dim_region
        WHERE LOWER(region_name) = LOWER(TRIM(COALESCE(rec.raw_region, '')));

        -- ---- ESTIMATE COST (70% of price if unknown) ----
        v_cost := ROUND(v_quantity * v_unit_price * 0.70, 2);

        -- ---- INSERT INTO FACT_SALES ----
        INSERT INTO fact_sales (
            order_id, customer_id, product_id, date_id,
            region_id, quantity, unit_price, discount_pct,
            cost, transaction_date
        ) VALUES (
            TRIM(rec.raw_order_id),
            v_customer_id,
            v_product_id,
            v_date_id,
            v_region_id,
            v_quantity,
            v_unit_price,
            v_discount,
            v_cost,
            v_clean_date
        );

        -- Mark row as successfully processed
        UPDATE stg_raw_transactions
        SET is_processed = TRUE, error_flag = NULL
        WHERE raw_id = rec.raw_id;

    END LOOP;

    RAISE NOTICE 'ETL complete. Check stg_raw_transactions.error_flag for rejected rows.';
END;
$$;

-- ============================================================
-- PART 5: VALIDATION QUERIES
-- Run these after loading to verify data quality
-- ============================================================

-- 1. Check for orphaned facts (missing dimension keys)
SELECT 'Orphaned customer_id' AS check_name, COUNT(*) AS count
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'Orphaned product_id', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'Orphaned date_id', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_date d ON f.date_id = d.date_id
WHERE d.date_id IS NULL;

-- 2. Revenue sanity check (no negative revenue rows)
SELECT COUNT(*) AS negative_revenue_count
FROM fact_sales
WHERE revenue < 0;

-- 3. NULL check on critical columns
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_qty
FROM fact_sales;

-- 4. Error summary from staging
SELECT error_flag, COUNT(*) AS rejected_rows
FROM stg_raw_transactions
WHERE error_flag IS NOT NULL
GROUP BY error_flag
ORDER BY rejected_rows DESC;

-- ============================================================
-- PART 6: CORE ANALYTICS VIEWS (for Power BI / Python intake)
-- ============================================================

-- Monthly Revenue Trend
CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT
    dd.year,
    dd.month_num,
    dd.month_name,
    SUM(fs.revenue)  AS total_revenue,
    SUM(fs.profit)   AS total_profit,
    COUNT(DISTINCT fs.order_id)   AS total_orders,
    COUNT(DISTINCT fs.customer_id) AS unique_customers
FROM fact_sales fs
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY dd.year, dd.month_num, dd.month_name
ORDER BY dd.year, dd.month_num;

-- Top Products by Revenue
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.quantity)  AS total_units_sold,
    SUM(fs.revenue)   AS total_revenue,
    RANK() OVER (ORDER BY SUM(fs.revenue) DESC) AS revenue_rank
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.product_name, dp.category;

-- Revenue by Region
CREATE OR REPLACE VIEW vw_revenue_by_region AS
SELECT
    dr.region_name,
    SUM(fs.revenue)  AS total_revenue,
    COUNT(DISTINCT fs.customer_id) AS unique_customers,
    ROUND(SUM(fs.revenue) * 100.0 / SUM(SUM(fs.revenue)) OVER (), 2) AS revenue_pct
FROM fact_sales fs
JOIN dim_region dr ON fs.region_id = dr.region_id
GROUP BY dr.region_name;

-- Customer Purchase Summary (base for RFM in Python)
CREATE OR REPLACE VIEW vw_customer_purchase_summary AS
SELECT
    dc.customer_id,
    dc.customer_key,
    dc.full_name,
    dc.email,
    dr.region_name,
    MAX(fs.transaction_date) AS last_purchase_date,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.revenue) AS total_revenue,
    AVG(fs.revenue) AS avg_order_value,
    MIN(fs.transaction_date) AS first_purchase_date
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
LEFT JOIN dim_region dr ON fs.region_id = dr.region_id
GROUP BY dc.customer_id, dc.customer_key, dc.full_name, dc.email, dr.region_name;

-- ============================================================
-- HOW TO EXECUTE
-- psql -U your_user -d your_db -f week1_schema_and_cleaning.sql
-- Then: CALL sp_clean_and_load_transactions();
-- ============================================================
