-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Silver — Cleansing, typing, de-duplication & rejects
-- MAGIC
-- MAGIC For every entity the notebook builds two tables:
-- MAGIC
-- MAGIC | Table | Content |
-- MAGIC |---|---|
-- MAGIC | `stg_<entity>` | **All** bronze rows, standardised and typed, with `dq_errors ARRAY<STRING>` and `is_valid` |
-- MAGIC | `<entity>` | Only the valid rows — the clean silver table used by gold |
-- MAGIC
-- MAGIC and finally **`etl_rejects`**: every invalid row of every entity, with its reject reason code(s) and the raw record as JSON.
-- MAGIC
-- MAGIC **Techniques:** `trim` / `initcap` / `nullif` for text · `try_cast` and `try_to_timestamp` so bad values become `NULL` instead of failing the job ·
-- MAGIC `ROW_NUMBER()` for de-duplication · `filter(array(CASE …))` to collect all failed rules per row (same pattern as Lab 02).
-- MAGIC
-- MAGIC **Idempotent:** all tables are rebuilt with `CREATE OR REPLACE TABLE … AS SELECT`; the result depends only on bronze.
-- MAGIC
-- MAGIC | Reject reason code | Entity | Rule |
-- MAGIC |---|---|---|
-- MAGIC | `MISSING_KEY` | all | Business key is NULL / blank |
-- MAGIC | `DUPLICATE_CUSTOMER` / `DUPLICATE_PRODUCT` / `DUPLICATE_STORE` / `DUPLICATE_ORDER` / `DUPLICATE_LINE` / `DUPLICATE_CHANGE` | all | 2nd+ copy of a business key |
-- MAGIC | `INVALID_PRODUCT` | products | Category or brand missing, or price/cost missing, ≤ 0 or negative |
-- MAGIC | `INVALID_ORDER_TS` | orders | Timestamp unparseable in every accepted format |
-- MAGIC | `INVALID_ORDER_STATUS` | orders | Status not in Completed / Returned / Cancelled |
-- MAGIC | `ORPHAN_ORDER` | order_items | `order_id` not found in valid orders |
-- MAGIC | `NON_POSITIVE_QUANTITY` | order_items | Quantity missing, not an integer or ≤ 0 |
-- MAGIC | `INVALID_UNIT_PRICE` | order_items | Unit price missing or ≤ 0 |
-- MAGIC | `INVALID_DISCOUNT` | order_items | Discount missing or outside 0–1 |
-- MAGIC | `ORPHAN_CUSTOMER` / `INVALID_EFFECTIVE_DATE` | customer_changes | Unknown customer / bad date |

-- COMMAND ----------

-- DBTITLE 1,Parameters
CREATE WIDGET TEXT catalog       DEFAULT 'urbanmart';
CREATE WIDGET TEXT bronze_schema DEFAULT 'bronze';
CREATE WIDGET TEXT silver_schema DEFAULT 'silver';
CREATE WIDGET TEXT gold_schema   DEFAULT 'gold';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:silver_schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Customers

-- COMMAND ----------

-- DBTITLE 1,stg_customers
CREATE OR REPLACE TABLE stg_customers
COMMENT 'SILVER staging: all bronze customers, standardised and typed, with DQ outcome (dq_errors, is_valid).'
AS
WITH typed AS (
  SELECT
    nullif(trim(customer_id), '')                                   AS customer_id,
    initcap(trim(first_name))                                       AS first_name,
    initcap(trim(last_name))                                        AS last_name,
    nullif(lower(trim(email)), '')                                  AS email,
    CASE upper(trim(gender)) WHEN 'M' THEN 'M' WHEN 'F' THEN 'F' END AS gender,
    CAST(try_to_timestamp(trim(birth_date),  'yyyy-MM-dd') AS DATE) AS birth_date,
    initcap(trim(city))                                             AS city,
    trim(region)                                                    AS region,
    trim(segment)                                                   AS segment,
    CAST(try_to_timestamp(trim(signup_date), 'yyyy-MM-dd') AS DATE) AS signup_date,
    to_json(struct(customer_id, first_name, last_name, email, gender,
                   birth_date, city, region, segment, signup_date)) AS _raw_record,
    _source_file,
    _ingested_at
  FROM IDENTIFIER(:bronze_schema || '.customers')
),
ranked AS (
  -- Exact duplicates: keep one row per customer_id (latest ingestion first).
  SELECT *,
         row_number() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC, signup_date DESC) AS _rn
  FROM typed
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN customer_id IS NULL                 THEN 'MISSING_KEY'        END,
           CASE WHEN customer_id IS NOT NULL AND _rn > 1 THEN 'DUPLICATE_CUSTOMER' END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn), size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,customers (clean)
CREATE OR REPLACE TABLE customers
COMMENT 'SILVER: cleansed, typed, de-duplicated customer master. One row per customer_id.'
AS
SELECT customer_id, first_name, last_name, email, gender, birth_date, city, region, segment, signup_date,
       _source_file, _ingested_at
FROM stg_customers
WHERE is_valid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Customer changes (input for SCD Type 2)

-- COMMAND ----------

-- DBTITLE 1,stg_customer_changes
CREATE OR REPLACE TABLE stg_customer_changes
COMMENT 'SILVER staging: customer change events, typed, with DQ outcome.'
AS
WITH typed AS (
  SELECT
    nullif(trim(ch.customer_id), '')                                      AS customer_id,
    initcap(trim(ch.new_city))                                            AS new_city,
    trim(ch.new_region)                                                   AS new_region,
    trim(ch.new_segment)                                                  AS new_segment,
    CAST(try_to_timestamp(trim(ch.effective_date), 'yyyy-MM-dd') AS DATE) AS effective_date,
    to_json(struct(ch.customer_id, ch.new_city, ch.new_region, ch.new_segment, ch.effective_date)) AS _raw_record,
    ch._source_file,
    ch._ingested_at
  FROM IDENTIFIER(:bronze_schema || '.customer_changes') AS ch
),
ranked AS (
  SELECT t.*,
         c.customer_id IS NOT NULL AS _customer_exists,
         row_number() OVER (PARTITION BY t.customer_id, t.effective_date ORDER BY t._ingested_at DESC) AS _rn
  FROM typed t
  LEFT JOIN customers c ON c.customer_id = t.customer_id
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN customer_id IS NULL                            THEN 'MISSING_KEY'            END,
           CASE WHEN customer_id IS NOT NULL AND NOT _customer_exists THEN 'ORPHAN_CUSTOMER'      END,
           CASE WHEN effective_date IS NULL                         THEN 'INVALID_EFFECTIVE_DATE' END,
           CASE WHEN _rn > 1                                        THEN 'DUPLICATE_CHANGE'       END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn, _customer_exists), size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,customer_changes (clean)
CREATE OR REPLACE TABLE customer_changes
COMMENT 'SILVER: valid customer change events (city / region / segment) with effective_date. Feeds dim_customer_scd2.'
AS
SELECT customer_id, new_city, new_region, new_segment, effective_date, _source_file, _ingested_at
FROM stg_customer_changes
WHERE is_valid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Products

-- COMMAND ----------

-- DBTITLE 1,stg_products
CREATE OR REPLACE TABLE stg_products
COMMENT 'SILVER staging: all bronze products, typed, with DQ outcome.'
AS
WITH typed AS (
  SELECT
    nullif(trim(product_id), '')                    AS product_id,
    trim(product_name)                              AS product_name,
    nullif(trim(category), '')                      AS category,
    nullif(trim(subcategory), '')                   AS subcategory,
    nullif(trim(brand), '')                         AS brand,
    try_cast(trim(unit_cost)  AS DECIMAL(10,2))     AS unit_cost,
    try_cast(trim(list_price) AS DECIMAL(10,2))     AS list_price,
    CASE upper(trim(is_active)) WHEN 'Y' THEN true WHEN 'N' THEN false END AS is_active,
    to_json(struct(product_id, product_name, category, subcategory, brand,
                   unit_cost, list_price, is_active)) AS _raw_record,
    _source_file,
    _ingested_at
  FROM IDENTIFIER(:bronze_schema || '.products')
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) AS _rn
  FROM typed
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN product_id IS NULL                 THEN 'MISSING_KEY'       END,
           CASE WHEN product_id IS NOT NULL AND _rn > 1 THEN 'DUPLICATE_PRODUCT' END,
           CASE WHEN category IS NULL OR brand IS NULL
                  OR list_price IS NULL OR list_price <= 0
                  OR unit_cost  IS NULL OR unit_cost  <  0 THEN 'INVALID_PRODUCT' END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn), size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,products (clean)
CREATE OR REPLACE TABLE products
COMMENT 'SILVER: cleansed product catalogue. One row per product_id. Placeholder records excluded.'
AS
SELECT product_id, product_name, category, subcategory, brand, unit_cost, list_price, is_active,
       _source_file, _ingested_at
FROM stg_products
WHERE is_valid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Stores

-- COMMAND ----------

-- DBTITLE 1,stg_stores
CREATE OR REPLACE TABLE stg_stores
COMMENT 'SILVER staging: all bronze stores, typed, with DQ outcome.'
AS
WITH typed AS (
  SELECT
    nullif(trim(store_id), '')                                      AS store_id,
    trim(store_name)                                                AS store_name,
    trim(store_type)                                                AS store_type,
    CASE WHEN lower(trim(store_type)) = 'online' THEN 'Online' ELSE 'Physical' END AS channel,
    initcap(trim(city))                                             AS city,
    trim(region)                                                    AS region,
    CAST(try_to_timestamp(trim(open_date), 'yyyy-MM-dd') AS DATE)   AS open_date,
    -- 0 m² is "not applicable" (online store), not a real size -> NULL.
    CASE WHEN try_cast(trim(size_sqm) AS INT) > 0 THEN try_cast(trim(size_sqm) AS INT) END AS size_sqm,
    to_json(struct(store_id, store_name, store_type, city, region, open_date, size_sqm)) AS _raw_record,
    _source_file,
    _ingested_at
  FROM IDENTIFIER(:bronze_schema || '.stores')
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _ingested_at DESC) AS _rn
  FROM typed
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN store_id IS NULL                 THEN 'MISSING_KEY'     END,
           CASE WHEN store_id IS NOT NULL AND _rn > 1 THEN 'DUPLICATE_STORE' END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn), size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,stores (clean)
CREATE OR REPLACE TABLE stores
COMMENT 'SILVER: cleansed store master (12 physical + 1 online). channel derived from store_type.'
AS
SELECT store_id, store_name, store_type, channel, city, region, open_date, size_sqm,
       _source_file, _ingested_at
FROM stg_stores
WHERE is_valid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Orders

-- COMMAND ----------

-- DBTITLE 1,stg_orders
CREATE OR REPLACE TABLE stg_orders
COMMENT 'SILVER staging: all bronze orders, typed, with DQ outcome.'
AS
WITH typed AS (
  SELECT
    nullif(trim(order_id), '')                                       AS order_id,
    -- Two formats exist in the extract: ISO (yyyy-MM-dd HH:mm:ss) and dd/MM/yyyy HH:mm.
    coalesce(try_to_timestamp(trim(order_ts), 'yyyy-MM-dd HH:mm:ss'),
             try_to_timestamp(trim(order_ts), 'dd/MM/yyyy HH:mm'))   AS order_ts,
    try_to_timestamp(trim(order_ts), 'yyyy-MM-dd HH:mm:ss') IS NULL
      AND try_to_timestamp(trim(order_ts), 'dd/MM/yyyy HH:mm') IS NOT NULL AS is_ts_reformatted,
    nullif(trim(customer_id), '')                                    AS customer_id,
    nullif(trim(customer_id), '') IS NULL                            AS is_guest,
    nullif(trim(store_id), '')                                       AS store_id,
    trim(payment_method)                                             AS payment_method,
    initcap(trim(order_status))                                      AS order_status,
    to_json(struct(order_id, order_ts, customer_id, store_id, payment_method, order_status)) AS _raw_record,
    _source_file,
    _ingested_at
  FROM IDENTIFIER(:bronze_schema || '.orders')
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) AS _rn
  FROM typed
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN order_id IS NULL                 THEN 'MISSING_KEY'          END,
           CASE WHEN order_id IS NOT NULL AND _rn > 1 THEN 'DUPLICATE_ORDER'      END,
           CASE WHEN order_ts IS NULL                 THEN 'INVALID_ORDER_TS'     END,
           CASE WHEN order_status IS NULL
                  OR order_status NOT IN ('Completed', 'Returned', 'Cancelled')
                                                      THEN 'INVALID_ORDER_STATUS' END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn), size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,orders (clean)
CREATE OR REPLACE TABLE orders
COMMENT 'SILVER: cleansed order headers. customer_id NULL = guest order. is_ts_reformatted = timestamp came in dd/MM/yyyy format.'
AS
SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, is_ts_reformatted,
       customer_id, is_guest, store_id, payment_method, order_status,
       _source_file, _ingested_at
FROM stg_orders
WHERE is_valid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Order items
-- MAGIC Line-level money measures are derived here, because they only need the line itself:
-- MAGIC `gross_amount = quantity × unit_price` · `discount_amount = round(gross × discount_pct, 2)` · `net_amount = gross − discount`.
-- MAGIC Cost and margin need the product cost and are derived in gold.

-- COMMAND ----------

-- DBTITLE 1,stg_order_items
CREATE OR REPLACE TABLE stg_order_items
COMMENT 'SILVER staging: all bronze order lines, typed, with DQ outcome.'
AS
WITH typed AS (
  SELECT
    nullif(trim(order_line_id), '')                AS order_line_id,
    nullif(trim(order_id), '')                     AS order_id,
    nullif(trim(product_id), '')                   AS product_id,
    try_cast(trim(quantity)     AS INT)            AS quantity,
    try_cast(trim(unit_price)   AS DECIMAL(10,2))  AS unit_price,
    try_cast(trim(discount_pct) AS DECIMAL(5,4))   AS discount_pct,
    to_json(struct(order_line_id, order_id, product_id, quantity, unit_price, discount_pct)) AS _raw_record,
    _source_file,
    _ingested_at
  FROM IDENTIFIER(:bronze_schema || '.order_items')
),
ranked AS (
  SELECT t.*,
         o.order_id   IS NOT NULL AS _order_exists,
         p.product_id IS NOT NULL AS _product_exists,
         row_number() OVER (PARTITION BY t.order_line_id ORDER BY t._ingested_at DESC) AS _rn
  FROM typed t
  LEFT JOIN orders   o ON o.order_id   = t.order_id
  LEFT JOIN products p ON p.product_id = t.product_id
),
checked AS (
  SELECT *,
         filter(array(
           CASE WHEN order_line_id IS NULL                 THEN 'MISSING_KEY'           END,
           CASE WHEN order_line_id IS NOT NULL AND _rn > 1 THEN 'DUPLICATE_LINE'        END,
           CASE WHEN NOT _order_exists                     THEN 'ORPHAN_ORDER'          END,
           CASE WHEN quantity IS NULL OR quantity <= 0     THEN 'NON_POSITIVE_QUANTITY' END,
           CASE WHEN unit_price IS NULL OR unit_price <= 0 THEN 'INVALID_UNIT_PRICE'    END,
           CASE WHEN discount_pct IS NULL
                  OR discount_pct NOT BETWEEN 0 AND 1      THEN 'INVALID_DISCOUNT'      END
         ), e -> e IS NOT NULL) AS dq_errors
  FROM ranked
)
SELECT * EXCEPT (_rn, _order_exists, _product_exists),
       NOT _product_exists AS is_unknown_product,
       size(dq_errors) = 0 AS is_valid
FROM checked;

-- COMMAND ----------

-- DBTITLE 1,order_items (clean, with line measures)
CREATE OR REPLACE TABLE order_items
COMMENT 'SILVER: valid order lines with gross / discount / net amounts. is_unknown_product = product_id not in the catalogue (kept, mapped to -1 in gold).'
AS
WITH m AS (
  SELECT *,
         CAST(quantity * unit_price AS DECIMAL(12,2))                 AS gross_amount,
         CAST(round(quantity * unit_price * discount_pct, 2) AS DECIMAL(12,2)) AS discount_amount
  FROM stg_order_items
  WHERE is_valid
)
SELECT order_line_id, order_id, product_id, is_unknown_product,
       quantity, unit_price, discount_pct,
       gross_amount,
       discount_amount,
       CAST(gross_amount - discount_amount AS DECIMAL(12,2)) AS net_amount,
       _source_file, _ingested_at
FROM m;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. ETL rejects

-- COMMAND ----------

-- DBTITLE 1,etl_rejects
CREATE OR REPLACE TABLE etl_rejects
COMMENT 'SILVER: every row rejected by the silver DQ rules. reject_reason = comma-separated reason codes; raw_record = original bronze values as JSON.'
AS
SELECT 'customers'        AS source_table, customer_id   AS source_key, array_join(dq_errors, ',') AS reject_reason, dq_errors, _raw_record AS raw_record, _source_file, _ingested_at, current_timestamp() AS rejected_at FROM stg_customers        WHERE NOT is_valid
UNION ALL
SELECT 'customer_changes', concat_ws('|', customer_id, CAST(effective_date AS STRING)), array_join(dq_errors, ','), dq_errors, _raw_record, _source_file, _ingested_at, current_timestamp() FROM stg_customer_changes WHERE NOT is_valid
UNION ALL
SELECT 'products',    product_id,    array_join(dq_errors, ','), dq_errors, _raw_record, _source_file, _ingested_at, current_timestamp() FROM stg_products    WHERE NOT is_valid
UNION ALL
SELECT 'stores',      store_id,      array_join(dq_errors, ','), dq_errors, _raw_record, _source_file, _ingested_at, current_timestamp() FROM stg_stores      WHERE NOT is_valid
UNION ALL
SELECT 'orders',      order_id,      array_join(dq_errors, ','), dq_errors, _raw_record, _source_file, _ingested_at, current_timestamp() FROM stg_orders      WHERE NOT is_valid
UNION ALL
SELECT 'order_items', order_line_id, array_join(dq_errors, ','), dq_errors, _raw_record, _source_file, _ingested_at, current_timestamp() FROM stg_order_items WHERE NOT is_valid;

-- COMMAND ----------

-- DBTITLE 1,Rejects by table and reason
SELECT source_table, reject_reason, count(*) AS rejected_rows
FROM etl_rejects
GROUP BY ALL
ORDER BY source_table, rejected_rows DESC;

-- COMMAND ----------

-- DBTITLE 1,Bronze -> silver row reconciliation
-- For every entity: bronze rows = silver rows + rejected rows.
WITH r AS (
  SELECT 'customers' AS entity,
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.customers')) AS bronze_rows,
         (SELECT count(*) FROM customers)                                  AS silver_rows,
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'customers') AS rejected_rows
  UNION ALL SELECT 'customer_changes',
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.customer_changes')),
         (SELECT count(*) FROM customer_changes),
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'customer_changes')
  UNION ALL SELECT 'products',
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.products')),
         (SELECT count(*) FROM products),
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'products')
  UNION ALL SELECT 'stores',
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.stores')),
         (SELECT count(*) FROM stores),
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'stores')
  UNION ALL SELECT 'orders',
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.orders')),
         (SELECT count(*) FROM orders),
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'orders')
  UNION ALL SELECT 'order_items',
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.order_items')),
         (SELECT count(*) FROM order_items),
         (SELECT count(*) FROM etl_rejects WHERE source_table = 'order_items')
)
SELECT *,
       assert_true(bronze_rows = silver_rows + rejected_rows,
                   concat('Silver reconciliation failed for ', entity)) AS assert_reconciled
FROM r;
