-- ============================================================
-- Consumer360: SQL Queries for Data Extraction & Preparation
-- ============================================================

-- -----------------------------------------------
-- 1. Raw Sales Data Extraction & Basic Cleansing
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_clean_transactions AS
SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE)       AS order_date,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    ROUND(quantity * unit_price, 2) AS line_total,
    region,
    country
FROM raw_sales_transactions
WHERE
    customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND quantity > 0
    AND unit_price > 0
    AND order_id IS NOT NULL;


-- -----------------------------------------------
-- 2. RFM Base Table
--    Calculates R, F, M per customer
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_rfm_base AS
SELECT
    customer_id,
    MAX(order_date)                                   AS last_purchase_date,
    DATEDIFF(CURRENT_DATE, MAX(order_date))           AS recency_days,
    COUNT(DISTINCT order_id)                          AS frequency,
    ROUND(SUM(line_total), 2)                         AS monetary
FROM vw_clean_transactions
GROUP BY customer_id;


-- -----------------------------------------------
-- 3. RFM Scoring (1–5 scale using NTILE)
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_rfm_scores AS
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    -- Lower recency = better, so we reverse rank
    6 - NTILE(5) OVER (ORDER BY recency_days ASC)    AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC)           AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC)            AS m_score
FROM vw_rfm_base;


-- -----------------------------------------------
-- 4. Cohort Analysis — First Purchase Month
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_cohort_base AS
SELECT
    customer_id,
    DATE_FORMAT(MIN(order_date), '%Y-%m-01')         AS cohort_month,
    DATE_FORMAT(order_date, '%Y-%m-01')              AS order_month
FROM vw_clean_transactions
GROUP BY customer_id, DATE_FORMAT(order_date, '%Y-%m-01');

CREATE OR REPLACE VIEW vw_cohort_retention AS
SELECT
    cohort_month,
    TIMESTAMPDIFF(MONTH, cohort_month, order_month)  AS months_since_acquisition,
    COUNT(DISTINCT customer_id)                      AS active_customers
FROM vw_cohort_base
GROUP BY cohort_month, months_since_acquisition
ORDER BY cohort_month, months_since_acquisition;


-- -----------------------------------------------
-- 5. Revenue & Sales Trend (Weekly Aggregate)
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_weekly_sales AS
SELECT
    YEARWEEK(order_date, 1)                          AS year_week,
    MIN(order_date)                                  AS week_start,
    COUNT(DISTINCT order_id)                         AS total_orders,
    COUNT(DISTINCT customer_id)                      AS unique_customers,
    ROUND(SUM(line_total), 2)                        AS total_revenue
FROM vw_clean_transactions
GROUP BY YEARWEEK(order_date, 1)
ORDER BY year_week;


-- -----------------------------------------------
-- 6. Top Products by Volume and Revenue
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    product_id,
    product_name,
    category,
    SUM(quantity)                                    AS total_units_sold,
    ROUND(SUM(line_total), 2)                        AS total_revenue,
    RANK() OVER (ORDER BY SUM(line_total) DESC)      AS revenue_rank,
    RANK() OVER (ORDER BY SUM(quantity) DESC)        AS volume_rank
FROM vw_clean_transactions
GROUP BY product_id, product_name, category;


-- -----------------------------------------------
-- 7. Revenue by Region
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_revenue_by_region AS
SELECT
    region,
    country,
    ROUND(SUM(line_total), 2)                        AS total_revenue,
    COUNT(DISTINCT customer_id)                      AS unique_customers,
    COUNT(DISTINCT order_id)                         AS total_orders
FROM vw_clean_transactions
GROUP BY region, country
ORDER BY total_revenue DESC;


-- -----------------------------------------------
-- 8. Market Basket — Item Pairs Extraction
-- -----------------------------------------------
CREATE OR REPLACE VIEW vw_order_products AS
SELECT
    order_id,
    product_id,
    product_name
FROM vw_clean_transactions;

-- Self-join to get product pairs per order (for Python to consume)
CREATE OR REPLACE VIEW vw_item_pairs AS
SELECT
    a.order_id,
    a.product_name AS product_a,
    b.product_name AS product_b
FROM vw_order_products a
JOIN vw_order_products b
    ON a.order_id = b.order_id
    AND a.product_id < b.product_id;
