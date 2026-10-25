-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Bronze — Data profiling & DQ log
-- MAGIC
-- MAGIC Profiles the bronze tables **before** any cleansing, so that every cleansing rule in silver is backed by evidence.
-- MAGIC
-- MAGIC Checks, per the capstone brief: **row counts · duplicates · NULL / blank values · text formatting · date formats ·
-- MAGIC numeric validity & ranges · referential integrity between files**.
-- MAGIC
-- MAGIC The last cell writes the findings to **`<bronze_schema>.dq_issue_log`** — one row per issue with its row count and handling rule.
-- MAGIC The same log is documented in [`docs/dq_log.md`](../docs/dq_log.md).

-- COMMAND ----------

-- DBTITLE 1,Parameters
CREATE WIDGET TEXT catalog       DEFAULT 'bootcamp';
CREATE WIDGET TEXT bronze_schema DEFAULT 'um_bronze_antonismp';
CREATE WIDGET TEXT silver_schema DEFAULT 'um_silver_antonismp';
CREATE WIDGET TEXT gold_schema   DEFAULT 'um_gold_antonismp';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:bronze_schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Row counts and key uniqueness

-- COMMAND ----------

-- DBTITLE 1,Rows vs. distinct business keys
SELECT 'customers'   AS table_name, count(*) AS total_rows, count(DISTINCT customer_id)   AS distinct_keys, count(*) - count(DISTINCT customer_id)   AS duplicate_key_rows FROM customers
UNION ALL SELECT 'customer_changes', count(*), count(DISTINCT customer_id, effective_date), count(*) - count(DISTINCT customer_id, effective_date) FROM customer_changes
UNION ALL SELECT 'products',    count(*), count(DISTINCT product_id),    count(*) - count(DISTINCT product_id)    FROM products
UNION ALL SELECT 'stores',      count(*), count(DISTINCT store_id),      count(*) - count(DISTINCT store_id)      FROM stores
UNION ALL SELECT 'orders',      count(*), count(DISTINCT order_id),      count(*) - count(DISTINCT order_id)      FROM orders
UNION ALL SELECT 'order_items', count(*), count(DISTINCT order_line_id), count(*) - count(DISTINCT order_line_id) FROM order_items;

-- COMMAND ----------

-- DBTITLE 1,Duplicate customers — are they exact copies?
-- If distinct_versions = 1 the duplicates are exact copies of the same record (safe to keep one).
SELECT
  customer_id,
  count(*) AS copies,
  count(DISTINCT to_json(struct(first_name, last_name, email, gender, birth_date, city, region, segment, signup_date))) AS distinct_versions
FROM customers
GROUP BY customer_id
HAVING count(*) > 1
ORDER BY customer_id;

-- COMMAND ----------

-- DBTITLE 1,Order lines with identical content inside the same order (investigation)
-- Same order, product, quantity, price and discount but a different order_line_id.
-- With ~3 random lines per order and 76 products, ~30 such pairs are expected by chance,
-- so these are treated as legitimate separate lines (the business key order_line_id is unique).
SELECT count(*)   AS groups_of_identical_lines,
       sum(c)     AS lines_involved,
       sum(c - 1) AS extra_lines
FROM (
  SELECT order_id, product_id, quantity, unit_price, discount_pct, count(*) AS c
  FROM order_items
  GROUP BY ALL
  HAVING count(*) > 1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. NULL / blank values
-- MAGIC The CSV reader turns empty fields into `NULL`; `trim(x) = ''` also catches whitespace-only values.

-- COMMAND ----------

-- DBTITLE 1,customers
SELECT
  count_if(customer_id IS NULL OR trim(customer_id) = '') AS customer_id_blank,
  count_if(first_name  IS NULL OR trim(first_name)  = '') AS first_name_blank,
  count_if(last_name   IS NULL OR trim(last_name)   = '') AS last_name_blank,
  count_if(email       IS NULL OR trim(email)       = '') AS email_blank,
  count_if(gender      IS NULL OR trim(gender)      = '') AS gender_blank,
  count_if(birth_date  IS NULL OR trim(birth_date)  = '') AS birth_date_blank,
  count_if(city        IS NULL OR trim(city)        = '') AS city_blank,
  count_if(region      IS NULL OR trim(region)      = '') AS region_blank,
  count_if(segment     IS NULL OR trim(segment)     = '') AS segment_blank,
  count_if(signup_date IS NULL OR trim(signup_date) = '') AS signup_date_blank
FROM customers;

-- COMMAND ----------

-- DBTITLE 1,products
SELECT
  count_if(product_name IS NULL OR trim(product_name) = '') AS product_name_blank,
  count_if(category     IS NULL OR trim(category)     = '') AS category_blank,
  count_if(subcategory  IS NULL OR trim(subcategory)  = '') AS subcategory_blank,
  count_if(brand        IS NULL OR trim(brand)        = '') AS brand_blank,
  count_if(unit_cost    IS NULL OR trim(unit_cost)    = '') AS unit_cost_blank,
  count_if(list_price   IS NULL OR trim(list_price)   = '') AS list_price_blank
FROM products;

-- COMMAND ----------

-- DBTITLE 1,The incomplete product record
SELECT * FROM products
WHERE category IS NULL OR brand IS NULL OR try_cast(list_price AS DECIMAL(10,2)) <= 0;

-- COMMAND ----------

-- DBTITLE 1,orders
SELECT
  count_if(order_id       IS NULL OR trim(order_id)       = '') AS order_id_blank,
  count_if(order_ts       IS NULL OR trim(order_ts)       = '') AS order_ts_blank,
  count_if(customer_id    IS NULL OR trim(customer_id)    = '') AS customer_id_blank_guest_orders,
  count_if(store_id       IS NULL OR trim(store_id)       = '') AS store_id_blank,
  count_if(payment_method IS NULL OR trim(payment_method) = '') AS payment_method_blank,
  count_if(order_status   IS NULL OR trim(order_status)   = '') AS order_status_blank
FROM orders;

-- COMMAND ----------

-- DBTITLE 1,order_items
SELECT
  count_if(order_id     IS NULL OR trim(order_id)     = '') AS order_id_blank,
  count_if(product_id   IS NULL OR trim(product_id)   = '') AS product_id_blank,
  count_if(quantity     IS NULL OR trim(quantity)     = '') AS quantity_blank,
  count_if(unit_price   IS NULL OR trim(unit_price)   = '') AS unit_price_blank,
  count_if(discount_pct IS NULL OR trim(discount_pct) = '') AS discount_pct_blank
FROM order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Text formatting and domain values

-- COMMAND ----------

-- DBTITLE 1,Customer city — raw values (look for case and trailing spaces)
SELECT concat('[', city, ']') AS city_raw_with_brackets, count(*) AS rows
FROM customers
GROUP BY city
ORDER BY city;

-- COMMAND ----------

-- DBTITLE 1,Rows whose city is not in clean "Title Case, trimmed" form
SELECT count(*) AS badly_formatted_city_rows
FROM customers
WHERE city <> initcap(trim(city));

-- COMMAND ----------

-- DBTITLE 1,Domain values of the low-cardinality columns
SELECT 'customers.gender' AS column_name, gender AS value, count(*) AS rows FROM customers GROUP BY ALL
UNION ALL SELECT 'customers.segment',     segment,        count(*) FROM customers   GROUP BY ALL
UNION ALL SELECT 'customers.region',      region,         count(*) FROM customers   GROUP BY ALL
UNION ALL SELECT 'products.category',     category,       count(*) FROM products    GROUP BY ALL
UNION ALL SELECT 'products.is_active',    is_active,      count(*) FROM products    GROUP BY ALL
UNION ALL SELECT 'stores.store_type',     store_type,     count(*) FROM stores      GROUP BY ALL
UNION ALL SELECT 'orders.payment_method', payment_method, count(*) FROM orders      GROUP BY ALL
UNION ALL SELECT 'orders.order_status',   order_status,   count(*) FROM orders      GROUP BY ALL
UNION ALL SELECT 'order_items.discount_pct', discount_pct, count(*) FROM order_items GROUP BY ALL
ORDER BY column_name, value;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Date and timestamp formats

-- COMMAND ----------

-- DBTITLE 1,order_ts — format patterns found in the file
-- Every digit is replaced with 9, so each distinct value is one "shape" of timestamp.
SELECT regexp_replace(order_ts, '[0-9]', '9') AS ts_pattern,
       count(*)                               AS rows,
       min(order_ts)                          AS example
FROM orders
GROUP BY ALL
ORDER BY rows DESC;

-- COMMAND ----------

-- DBTITLE 1,order_ts — parse result per format
SELECT
  count_if(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss') IS NOT NULL)                       AS iso_format_rows,
  count_if(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss') IS NULL
           AND try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm') IS NOT NULL)                      AS ddmmyyyy_format_rows,
  count_if(coalesce(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss'),
                    try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm')) IS NULL)                    AS unparseable_rows,
  min(coalesce(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss'),
               try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm')))                                 AS min_order_ts,
  max(coalesce(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss'),
               try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm')))                                 AS max_order_ts
FROM orders;

-- COMMAND ----------

-- DBTITLE 1,Other dates (customers, stores, changes) — all ISO?
SELECT
  (SELECT count_if(try_to_timestamp(birth_date,  'yyyy-MM-dd') IS NULL) FROM customers)        AS bad_birth_date,
  (SELECT count_if(try_to_timestamp(signup_date, 'yyyy-MM-dd') IS NULL) FROM customers)        AS bad_signup_date,
  (SELECT count_if(try_to_timestamp(open_date,   'yyyy-MM-dd') IS NULL) FROM stores)           AS bad_open_date,
  (SELECT count_if(try_to_timestamp(effective_date, 'yyyy-MM-dd') IS NULL) FROM customer_changes) AS bad_effective_date,
  (SELECT min(birth_date) || ' .. ' || max(birth_date) FROM customers)                         AS birth_date_range,
  (SELECT min(effective_date) || ' .. ' || max(effective_date) FROM customer_changes)          AS change_date_range;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Numeric validity and ranges

-- COMMAND ----------

-- DBTITLE 1,order_items numeric checks
SELECT
  count_if(try_cast(quantity AS INT) IS NULL)                     AS quantity_not_integer,
  count_if(try_cast(quantity AS INT) <= 0)                        AS quantity_not_positive,
  count_if(try_cast(unit_price AS DECIMAL(10,2)) IS NULL)         AS unit_price_not_numeric,
  count_if(try_cast(unit_price AS DECIMAL(10,2)) <= 0)            AS unit_price_not_positive,
  count_if(try_cast(discount_pct AS DECIMAL(5,4)) NOT BETWEEN 0 AND 1) AS discount_out_of_range,
  min(try_cast(quantity AS INT))                                  AS min_quantity,
  max(try_cast(quantity AS INT))                                  AS max_quantity,
  min(try_cast(unit_price AS DECIMAL(10,2)))                      AS min_unit_price,
  max(try_cast(unit_price AS DECIMAL(10,2)))                      AS max_unit_price
FROM order_items;

-- COMMAND ----------

-- DBTITLE 1,Negative quantities by order status (are they returns?)
-- Most negative quantities sit on "Completed" orders, so they are not return lines: they are data errors.
SELECT o.order_status, count(*) AS negative_quantity_lines
FROM order_items i
JOIN orders o ON o.order_id = i.order_id
WHERE try_cast(i.quantity AS INT) < 0
GROUP BY ALL;

-- COMMAND ----------

-- DBTITLE 1,products and stores numeric checks
SELECT
  (SELECT count_if(try_cast(unit_cost AS DECIMAL(10,2)) IS NULL OR try_cast(list_price AS DECIMAL(10,2)) IS NULL) FROM products) AS product_price_not_numeric,
  (SELECT count_if(try_cast(list_price AS DECIMAL(10,2)) <= 0)                                     FROM products) AS product_price_zero,
  (SELECT count_if(try_cast(unit_cost AS DECIMAL(10,2)) >= try_cast(list_price AS DECIMAL(10,2))) FROM products) AS product_cost_ge_price,
  (SELECT count_if(try_cast(size_sqm AS INT) <= 0)                                                 FROM stores)   AS store_size_zero;

-- COMMAND ----------

-- DBTITLE 1,Store with zero floor area
SELECT * FROM stores WHERE try_cast(size_sqm AS INT) <= 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Referential integrity between files

-- COMMAND ----------

-- DBTITLE 1,Orphan checks
SELECT 'orders.customer_id -> customers' AS relationship,
       count(*) AS orphan_rows
FROM orders o LEFT ANTI JOIN customers c ON c.customer_id = o.customer_id
WHERE o.customer_id IS NOT NULL
UNION ALL
SELECT 'orders.store_id -> stores', count(*)
FROM orders o LEFT ANTI JOIN stores s ON s.store_id = o.store_id
UNION ALL
SELECT 'order_items.order_id -> orders', count(*)
FROM order_items i LEFT ANTI JOIN orders o ON o.order_id = i.order_id
UNION ALL
SELECT 'order_items.product_id -> products', count(*)
FROM order_items i LEFT ANTI JOIN products p ON p.product_id = i.product_id
UNION ALL
SELECT 'customer_changes.customer_id -> customers', count(*)
FROM customer_changes ch LEFT ANTI JOIN customers c ON c.customer_id = ch.customer_id
UNION ALL
SELECT 'orders without any order line', count(*)
FROM orders o LEFT ANTI JOIN order_items i ON i.order_id = o.order_id;

-- COMMAND ----------

-- DBTITLE 1,The orphan order lines
SELECT i.*,
       o.order_id IS NULL AS order_missing,
       p.product_id IS NULL AS product_missing
FROM order_items i
LEFT JOIN orders   o ON o.order_id   = i.order_id
LEFT JOIN products p ON p.product_id = i.product_id
WHERE o.order_id IS NULL OR p.product_id IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. DQ issue log
-- MAGIC One row per issue. Counts are **computed from the data**, so the log stays correct if the source extract changes.

-- COMMAND ----------

-- DBTITLE 1,Write dq_issue_log
CREATE OR REPLACE TABLE dq_issue_log
COMMENT 'Data-quality issue log of the bronze layer: one row per issue, with affected row count and handling rule.'
AS
SELECT issue_id, source_table, column_name, issue, affected_rows, severity, handling_rule, reject_reason_code,
       current_timestamp() AS profiled_at
FROM (
  SELECT 'DQ01' AS issue_id, 'customers' AS source_table, 'customer_id' AS column_name,
         'Exact duplicate customer records (same customer_id, identical attributes)' AS issue,
         (SELECT count(*) - count(DISTINCT customer_id) FROM customers) AS affected_rows,
         'High' AS severity,
         'Keep one record per customer_id (ROW_NUMBER); extra copies go to etl_rejects' AS handling_rule,
         'DUPLICATE_CUSTOMER' AS reject_reason_code
  UNION ALL
  SELECT 'DQ02', 'customers', 'city',
         'City in UPPER CASE with trailing whitespace (e.g. "IOANNINA ")',
         (SELECT count(*) FROM customers WHERE city <> initcap(trim(city))),
         'Medium', 'Standardise with initcap(trim(city))', NULL
  UNION ALL
  SELECT 'DQ03', 'customers', 'email',
         'Missing e-mail address',
         (SELECT count_if(email IS NULL OR trim(email) = '') FROM customers),
         'Low', 'Keep the customer; email stored as NULL (not needed for analytics)', NULL
  UNION ALL
  SELECT 'DQ04', 'products', 'category, subcategory, brand, unit_cost, list_price',
         'Placeholder product P9999 "Unknown Item": blank category/subcategory/brand, cost and price = 0',
         (SELECT count(*) FROM products WHERE category IS NULL OR brand IS NULL OR try_cast(list_price AS DECIMAL(10,2)) <= 0),
         'Medium', 'Exclude from dim_product (etl_rejects); the gold unknown member -1 plays this role', 'INVALID_PRODUCT'
  UNION ALL
  SELECT 'DQ05', 'stores', 'size_sqm',
         'Online store S900 has size_sqm = 0',
         (SELECT count(*) FROM stores WHERE try_cast(size_sqm AS INT) <= 0),
         'Low', 'Set size_sqm to NULL (not applicable to the online channel) so averages are not distorted', NULL
  UNION ALL
  SELECT 'DQ06', 'orders', 'customer_id',
         'Blank customer_id (guest checkout)',
         (SELECT count_if(customer_id IS NULL OR trim(customer_id) = '') FROM orders),
         'Medium', 'Keep the order; customer_key = -1 (Guest / Unknown member)', NULL
  UNION ALL
  SELECT 'DQ07', 'orders', 'order_ts',
         'Timestamp in dd/MM/yyyy HH:mm instead of yyyy-MM-dd HH:mm:ss (time always 12:00)',
         (SELECT count_if(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss') IS NULL) FROM orders),
         'High', 'Parse with coalesce(try_to_timestamp(ISO), try_to_timestamp(dd/MM/yyyy HH:mm)); flag is_ts_reformatted; unparseable -> etl_rejects', 'INVALID_ORDER_TS'
  UNION ALL
  SELECT 'DQ08', 'order_items', 'quantity',
         'Negative quantity (mostly on Completed orders, so not returns)',
         (SELECT count_if(try_cast(quantity AS INT) <= 0) FROM order_items),
         'High', 'Reject the line to etl_rejects; fact CHECK constraint quantity > 0', 'NON_POSITIVE_QUANTITY'
  UNION ALL
  SELECT 'DQ09', 'order_items', 'order_id',
         'Order line whose order_id does not exist in orders (O999999)',
         (SELECT count(*) FROM order_items i LEFT ANTI JOIN orders o ON o.order_id = i.order_id),
         'High', 'Reject the line to etl_rejects (no date, store or customer can be derived)', 'ORPHAN_ORDER'
  UNION ALL
  SELECT 'DQ10', 'order_items', 'product_id',
         'Order line with a product_id missing from the catalogue (PX000)',
         (SELECT count(*) FROM order_items i LEFT ANTI JOIN products p ON p.product_id = i.product_id),
         'Medium', 'Keep the sale (revenue is real); product_key = -1 (Unknown product); cost and margin NULL because cost is unknown', NULL
  UNION ALL
  SELECT 'DQ11', 'order_items', 'order_id, product_id, quantity, unit_price, discount_pct',
         'Lines with identical content inside the same order but different order_line_id (investigated)',
         (SELECT sum(c - 1) FROM (SELECT order_id, product_id, quantity, unit_price, discount_pct, count(*) AS c FROM order_items GROUP BY ALL HAVING count(*) > 1)),
         'Info', 'Not a defect: rate matches random chance and order_line_id is unique -> kept', NULL
  UNION ALL
  SELECT 'DQ12', 'orders', 'order_status',
         'Cancelled and Returned orders present in the sales extract',
         (SELECT count_if(order_status IN ('Cancelled', 'Returned')) FROM orders),
         'Info', 'Business rule, not a defect: loaded to the fact with status in the order-profile dimension; revenue KPIs use Completed only', NULL
);

-- COMMAND ----------

SELECT * FROM dq_issue_log ORDER BY issue_id;