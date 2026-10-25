-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Validation — the quality gate
-- MAGIC
-- MAGIC Last task of the Lakeflow Job. Every check is written to **`<gold_schema>.etl_validation_log`** (one row per check per run)
-- MAGIC and then enforced with **`assert_true()`**: if any check fails, the query raises an error, the notebook fails and the job run is marked *Failed*.
-- MAGIC
-- MAGIC | # | Check | Why |
-- MAGIC |---|---|---|
-- MAGIC | V01 | bronze order lines − rejected order lines = fact rows | No line is lost or duplicated between layers |
-- MAGIC | V02–V03 | Σ `net_amount` and Σ `quantity`: silver = gold | Money and units reconcile |
-- MAGIC | V04–V09 | No orphan foreign keys (6 FKs) | UC does **not** enforce FOREIGN KEY constraints |
-- MAGIC | V10–V15 | No duplicate surrogate / primary keys | UC does **not** enforce PRIMARY KEY constraints |
-- MAGIC | V16–V20 | No duplicate business keys in dimensions | One member per business key (per version for SCD2) |
-- MAGIC | V21 | Exactly one unknown member (-1) per dimension | Fallback rows exist for the key lookups |
-- MAGIC | V22–V24 | SCD2: one current version per customer, no gaps / overlaps, point-in-time key belongs to the same customer | Correct history |
-- MAGIC | V25 | `margin_amount = net_amount − cost_amount` | Derived measures are consistent |
-- MAGIC | V26 | Every rejected row has a reason code | Rejects are explainable |
-- MAGIC | V27 | Σ completed net revenue: `fact_sales` = `fact_sales_monthly` | Aggregate fact reconciles to the detail |

-- COMMAND ----------

-- DBTITLE 1,Parameters
CREATE WIDGET TEXT catalog       DEFAULT 'bootcamp';
CREATE WIDGET TEXT bronze_schema DEFAULT 'um_bronze_antonismp';
CREATE WIDGET TEXT silver_schema DEFAULT 'um_silver_antonismp';
CREATE WIDGET TEXT gold_schema   DEFAULT 'um_gold_antonismp';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:gold_schema);

-- COMMAND ----------

-- DBTITLE 1,Validation log table
CREATE TABLE IF NOT EXISTS etl_validation_log (
  run_ts      TIMESTAMP NOT NULL COMMENT 'Validation run timestamp (same for all checks of one run).',
  check_id    STRING    NOT NULL COMMENT 'V01, V02, ...',
  check_name  STRING    NOT NULL COMMENT 'What is tested.',
  expected    STRING             COMMENT 'Expected value.',
  actual      STRING             COMMENT 'Actual value.',
  passed      BOOLEAN   NOT NULL COMMENT 'TRUE when expected = actual.'
)
COMMENT 'GOLD: history of validation checks, one row per check per pipeline run.';

-- COMMAND ----------

-- DBTITLE 1,Run all checks and log the results
INSERT INTO etl_validation_log
SELECT current_timestamp() AS run_ts, check_id, check_name,
       -- show whole numbers without ".00", keep decimals for money
       CASE WHEN expected = floor(expected) THEN CAST(CAST(expected AS BIGINT) AS STRING) ELSE CAST(expected AS STRING) END,
       CASE WHEN actual   = floor(actual)   THEN CAST(CAST(actual   AS BIGINT) AS STRING) ELSE CAST(actual   AS STRING) END,
       coalesce(expected = actual, false) AS passed
FROM (
  -- V01 · line reconciliation
  SELECT 'V01' AS check_id,
         'bronze order lines - rejected order lines = fact_sales rows' AS check_name,
         (SELECT count(*) FROM IDENTIFIER(:bronze_schema || '.order_items'))
           - (SELECT count(*) FROM IDENTIFIER(:silver_schema || '.etl_rejects') WHERE source_table = 'order_items') AS expected,
         (SELECT count(*) FROM fact_sales) AS actual

  -- V02-V03 · amounts reconcile
  UNION ALL SELECT 'V02', 'sum(net_amount): silver.order_items = gold.fact_sales',
         (SELECT sum(net_amount) FROM IDENTIFIER(:silver_schema || '.order_items')),
         (SELECT sum(net_amount) FROM fact_sales)
  UNION ALL SELECT 'V03', 'sum(quantity): silver.order_items = gold.fact_sales',
         (SELECT sum(quantity) FROM IDENTIFIER(:silver_schema || '.order_items')),
         (SELECT sum(quantity) FROM fact_sales)

  -- V04-V09 · orphan foreign keys
  UNION ALL SELECT 'V04', 'orphan fact_sales.date_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_date d ON d.date_key = f.date_key)
  UNION ALL SELECT 'V05', 'orphan fact_sales.customer_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_customer d ON d.customer_key = f.customer_key)
  UNION ALL SELECT 'V06', 'orphan fact_sales.customer_version_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_customer_scd2 d ON d.customer_version_key = f.customer_version_key)
  UNION ALL SELECT 'V07', 'orphan fact_sales.product_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_product d ON d.product_key = f.product_key)
  UNION ALL SELECT 'V08', 'orphan fact_sales.store_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_store d ON d.store_key = f.store_key)
  UNION ALL SELECT 'V09', 'orphan fact_sales.order_profile_key', 0,
         (SELECT count(*) FROM fact_sales f LEFT ANTI JOIN dim_order_profile d ON d.order_profile_key = f.order_profile_key)

  -- V10-V15 · duplicate primary keys
  UNION ALL SELECT 'V10', 'duplicate dim_date.date_key', 0,
         (SELECT count(*) - count(DISTINCT date_key) FROM dim_date)
  UNION ALL SELECT 'V11', 'duplicate dim_customer.customer_key', 0,
         (SELECT count(*) - count(DISTINCT customer_key) FROM dim_customer)
  UNION ALL SELECT 'V12', 'duplicate dim_customer_scd2.customer_version_key', 0,
         (SELECT count(*) - count(DISTINCT customer_version_key) FROM dim_customer_scd2)
  UNION ALL SELECT 'V13', 'duplicate dim_product.product_key', 0,
         (SELECT count(*) - count(DISTINCT product_key) FROM dim_product)
  UNION ALL SELECT 'V14', 'duplicate dim_store.store_key / dim_order_profile.order_profile_key', 0,
         (SELECT count(*) - count(DISTINCT store_key) FROM dim_store)
         + (SELECT count(*) - count(DISTINCT order_profile_key) FROM dim_order_profile)
  UNION ALL SELECT 'V15', 'duplicate fact_sales.order_line_id', 0,
         (SELECT count(*) - count(DISTINCT order_line_id) FROM fact_sales)

  -- V16-V20 · duplicate business keys
  UNION ALL SELECT 'V16', 'duplicate dim_customer.customer_id', 0,
         (SELECT count(*) - count(DISTINCT customer_id) FROM dim_customer)
  UNION ALL SELECT 'V17', 'duplicate dim_customer_scd2 (customer_id, valid_from)', 0,
         (SELECT count(*) - count(DISTINCT customer_id, valid_from) FROM dim_customer_scd2)
  UNION ALL SELECT 'V18', 'duplicate dim_product.product_id', 0,
         (SELECT count(*) - count(DISTINCT product_id) FROM dim_product)
  UNION ALL SELECT 'V19', 'duplicate dim_store.store_id', 0,
         (SELECT count(*) - count(DISTINCT store_id) FROM dim_store)
  UNION ALL SELECT 'V20', 'duplicate dim_order_profile (payment_method, order_status)', 0,
         (SELECT count(*) - count(DISTINCT payment_method, order_status) FROM dim_order_profile)

  -- V21 · unknown members
  UNION ALL SELECT 'V21', 'dimensions having exactly one unknown member (-1)', 6,
         (SELECT count_if(n = 1) FROM (
            SELECT count_if(date_key = -1) AS n FROM dim_date
            UNION ALL SELECT count_if(customer_key = -1)         FROM dim_customer
            UNION ALL SELECT count_if(customer_version_key = -1) FROM dim_customer_scd2
            UNION ALL SELECT count_if(product_key = -1)          FROM dim_product
            UNION ALL SELECT count_if(store_key = -1)            FROM dim_store
            UNION ALL SELECT count_if(order_profile_key = -1)    FROM dim_order_profile))

  -- V22-V24 · SCD Type 2 integrity
  UNION ALL SELECT 'V22', 'customers without exactly one current SCD2 version', 0,
         (SELECT count(*) FROM (
            SELECT customer_id FROM dim_customer_scd2 GROUP BY customer_id HAVING count_if(is_current) <> 1))
  UNION ALL SELECT 'V23', 'SCD2 versions with a gap or overlap to the next version', 0,
         (SELECT count(*) FROM (
            SELECT valid_to,
                   lead(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
            FROM dim_customer_scd2)
          WHERE next_from IS NOT NULL AND next_from <> date_add(valid_to, 1))
  UNION ALL SELECT 'V24', 'fact rows whose SCD2 version belongs to another customer', 0,
         (SELECT count(*)
          FROM fact_sales f
          JOIN dim_customer      c  ON c.customer_key          = f.customer_key
          JOIN dim_customer_scd2 cv ON cv.customer_version_key = f.customer_version_key
          WHERE c.customer_id <> cv.customer_id)

  -- V25 · derived measures
  UNION ALL SELECT 'V25', 'fact rows where margin_amount <> net_amount - cost_amount', 0,
         (SELECT count(*) FROM fact_sales
          WHERE cost_amount IS NOT NULL AND margin_amount <> net_amount - cost_amount)

  -- V26 · rejects are explained
  UNION ALL SELECT 'V26', 'rejected rows without a reason code', 0,
         (SELECT count_if(reject_reason IS NULL OR reject_reason = '') FROM IDENTIFIER(:silver_schema || '.etl_rejects'))

  -- V27 · aggregate fact
  UNION ALL SELECT 'V27', 'completed net revenue: fact_sales = fact_sales_monthly',
         (SELECT sum(f.net_amount) FROM fact_sales f
            JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
           WHERE op.is_completed),
         (SELECT sum(net_revenue) FROM fact_sales_monthly)
);

-- COMMAND ----------

-- DBTITLE 1,Results of this run
SELECT check_id, check_name, expected, actual, passed
FROM etl_validation_log
WHERE run_ts = (SELECT max(run_ts) FROM etl_validation_log)
ORDER BY check_id;

-- COMMAND ----------

-- DBTITLE 1,GATE — fail the notebook (and the job) if any check failed
SELECT
  check_id,
  assert_true(passed, concat('Validation ', check_id, ' failed: ', check_name,
                             ' (expected ', coalesce(expected, 'NULL'), ', actual ', coalesce(actual, 'NULL'), ')')) AS gate
FROM etl_validation_log
WHERE run_ts = (SELECT max(run_ts) FROM etl_validation_log)
ORDER BY check_id;