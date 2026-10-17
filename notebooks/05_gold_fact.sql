-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Gold — `fact_sales` and `fact_sales_monthly`
-- MAGIC
-- MAGIC **Grain of `fact_sales`: one row per order line (`order_line_id`) of a valid UrbanMart order.**
-- MAGIC
-- MAGIC | Column group | Columns |
-- MAGIC |---|---|
-- MAGIC | Degenerate dimensions | `order_line_id` (PK), `order_id` |
-- MAGIC | Foreign keys | `date_key`, `customer_key`, `customer_version_key` (SCD2, point-in-time), `product_key`, `store_key`, `order_profile_key` |
-- MAGIC | Measures (additive) | `quantity`, `gross_amount`, `discount_amount`, `net_amount`, `cost_amount`, `margin_amount` |
-- MAGIC | Non-additive facts | `unit_price`, `discount_pct`, `unit_cost` (never SUM them — derive ratios from the additive measures) |
-- MAGIC
-- MAGIC **Load:** surrogate-key lookups with `LEFT JOIN` + `coalesce(…, -1)` (a missing dimension member never drops a sale),
-- MAGIC then `MERGE INTO` on `order_line_id`. The silver extract is a full snapshot, so the MERGE also deletes fact rows whose
-- MAGIC line no longer exists (`WHEN NOT MATCHED BY SOURCE`). Re-running the notebook changes nothing → **idempotent**.
-- MAGIC
-- MAGIC **Constraints:** `PRIMARY KEY` / `FOREIGN KEY` are informational in Unity Catalog (tested in notebook 06);
-- MAGIC the `CHECK` constraints below are **enforced** by Delta on every write.
-- MAGIC **Layout:** liquid clustering `CLUSTER BY (date_key)` — most queries filter or group by date.

-- COMMAND ----------

-- DBTITLE 1,Parameters
CREATE WIDGET TEXT catalog       DEFAULT 'urbanmart';
CREATE WIDGET TEXT bronze_schema DEFAULT 'bronze';
CREATE WIDGET TEXT silver_schema DEFAULT 'silver';
CREATE WIDGET TEXT gold_schema   DEFAULT 'gold';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA  IDENTIFIER(:gold_schema);

-- COMMAND ----------

-- DBTITLE 1,Create fact_sales (PK / FK, liquid clustering)
CREATE TABLE IF NOT EXISTS fact_sales (
  order_line_id         STRING        NOT NULL COMMENT 'Degenerate dimension: order line business key (grain).',
  order_id              STRING        NOT NULL COMMENT 'Degenerate dimension: order number.',
  date_key              INT           NOT NULL COMMENT 'FK -> dim_date (order date).',
  customer_key          BIGINT        NOT NULL COMMENT 'FK -> dim_customer (current attributes); -1 = guest.',
  customer_version_key  BIGINT        NOT NULL COMMENT 'FK -> dim_customer_scd2: customer version valid on the order date (point-in-time).',
  product_key           BIGINT        NOT NULL COMMENT 'FK -> dim_product; -1 = product not in catalogue.',
  store_key             BIGINT        NOT NULL COMMENT 'FK -> dim_store.',
  order_profile_key     BIGINT        NOT NULL COMMENT 'FK -> dim_order_profile (payment method + order status).',
  order_ts              TIMESTAMP     NOT NULL COMMENT 'Order timestamp.',
  quantity              INT           NOT NULL COMMENT 'Units sold (additive).',
  unit_price            DECIMAL(10,2) NOT NULL COMMENT 'Selling price per unit (non-additive).',
  discount_pct          DECIMAL(5,4)  NOT NULL COMMENT 'Discount rate 0-1 (non-additive).',
  unit_cost             DECIMAL(10,2)          COMMENT 'Product cost per unit at load time (non-additive); NULL if product unknown.',
  gross_amount          DECIMAL(12,2) NOT NULL COMMENT 'quantity * unit_price (additive).',
  discount_amount       DECIMAL(12,2) NOT NULL COMMENT 'round(gross_amount * discount_pct, 2) (additive).',
  net_amount            DECIMAL(12,2) NOT NULL COMMENT 'gross_amount - discount_amount (additive).',
  cost_amount           DECIMAL(12,2)          COMMENT 'quantity * unit_cost (additive); NULL if product unknown.',
  margin_amount         DECIMAL(12,2)          COMMENT 'net_amount - cost_amount (additive); NULL if product unknown.',
  _record_hash          STRING        NOT NULL COMMENT 'SHA-256 of keys + measures; MERGE only rewrites rows that really changed.',
  _loaded_at            TIMESTAMP     NOT NULL COMMENT 'When the row was written.',
  CONSTRAINT pk_fact_sales                  PRIMARY KEY (order_line_id),
  CONSTRAINT fk_fact_sales_date             FOREIGN KEY (date_key)             REFERENCES dim_date (date_key),
  CONSTRAINT fk_fact_sales_customer         FOREIGN KEY (customer_key)         REFERENCES dim_customer (customer_key),
  CONSTRAINT fk_fact_sales_customer_version FOREIGN KEY (customer_version_key) REFERENCES dim_customer_scd2 (customer_version_key),
  CONSTRAINT fk_fact_sales_product          FOREIGN KEY (product_key)          REFERENCES dim_product (product_key),
  CONSTRAINT fk_fact_sales_store            FOREIGN KEY (store_key)            REFERENCES dim_store (store_key),
  CONSTRAINT fk_fact_sales_order_profile    FOREIGN KEY (order_profile_key)    REFERENCES dim_order_profile (order_profile_key)
)
CLUSTER BY (date_key)
COMMENT 'GOLD: UrbanMart sales fact. Grain: one row per order line.'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

-- DBTITLE 1,CHECK constraints (enforced by Delta) — drop + add, so the cell is re-runnable
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS ck_fact_quantity_positive;
ALTER TABLE fact_sales ADD  CONSTRAINT ck_fact_quantity_positive CHECK (quantity > 0);

ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS ck_fact_unit_price_positive;
ALTER TABLE fact_sales ADD  CONSTRAINT ck_fact_unit_price_positive CHECK (unit_price > 0);

ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS ck_fact_discount_range;
ALTER TABLE fact_sales ADD  CONSTRAINT ck_fact_discount_range CHECK (discount_pct BETWEEN 0 AND 1);

ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS ck_fact_net_amount;
ALTER TABLE fact_sales ADD  CONSTRAINT ck_fact_net_amount CHECK (net_amount >= 0 AND net_amount = gross_amount - discount_amount);

-- COMMAND ----------

-- DBTITLE 1,Load fact_sales (surrogate-key lookups + MERGE)
MERGE INTO fact_sales AS t
USING (
  WITH src AS (
    SELECT
      i.order_line_id,
      i.order_id,
      coalesce(d.date_key, -1)                     AS date_key,
      coalesce(c.customer_key, -1)                 AS customer_key,
      coalesce(cv.customer_version_key, -1)        AS customer_version_key,
      coalesce(p.product_key, -1)                  AS product_key,
      coalesce(s.store_key, -1)                    AS store_key,
      coalesce(op.order_profile_key, -1)           AS order_profile_key,
      o.order_ts,
      i.quantity,
      i.unit_price,
      i.discount_pct,
      p.unit_cost,
      i.gross_amount,
      i.discount_amount,
      i.net_amount,
      CAST(i.quantity * p.unit_cost AS DECIMAL(12,2))                AS cost_amount,
      CAST(i.net_amount - i.quantity * p.unit_cost AS DECIMAL(12,2)) AS margin_amount
    FROM IDENTIFIER(:silver_schema || '.order_items') AS i
    JOIN IDENTIFIER(:silver_schema || '.orders')      AS o  ON o.order_id = i.order_id
    LEFT JOIN dim_date          AS d  ON d.full_date    = o.order_date
    LEFT JOIN dim_customer      AS c  ON c.customer_id  = o.customer_id
    LEFT JOIN dim_customer_scd2 AS cv ON cv.customer_id = o.customer_id
                                     AND o.order_date BETWEEN cv.valid_from AND cv.valid_to
    LEFT JOIN dim_product       AS p  ON p.product_id   = i.product_id
    LEFT JOIN dim_store         AS s  ON s.store_id     = o.store_id
    LEFT JOIN dim_order_profile AS op ON op.payment_method = o.payment_method
                                     AND op.order_status   = o.order_status
  )
  SELECT *,
         sha2(to_json(struct(*)), 256) AS _record_hash,
         current_timestamp()           AS _loaded_at
  FROM src
) AS s
ON t.order_line_id = s.order_line_id
WHEN MATCHED AND t._record_hash <> s._record_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------

-- DBTITLE 1,Lines mapped to unknown members (expected: guests and the unknown product)
SELECT
  count(*)                              AS fact_rows,
  count_if(customer_key = -1)           AS guest_or_unknown_customer_lines,
  count_if(customer_version_key = -1)   AS unknown_customer_version_lines,
  count_if(product_key = -1)            AS unknown_product_lines,
  count_if(store_key = -1)              AS unknown_store_lines,
  count_if(date_key = -1)               AS unknown_date_lines,
  count_if(order_profile_key = -1)      AS unknown_profile_lines
FROM fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregate fact `fact_sales_monthly` (stretch goal)
-- MAGIC **Grain: one row per month × store × product category.** Pre-aggregated for the dashboard; derived only from `fact_sales`,
-- MAGIC so it is rebuilt in full (`CREATE OR REPLACE`) on every run.
-- MAGIC Revenue rule: *Completed* orders count as revenue, *Returned* orders are reported separately, *Cancelled* orders are excluded.

-- COMMAND ----------

-- DBTITLE 1,Build fact_sales_monthly
CREATE OR REPLACE TABLE fact_sales_monthly
CLUSTER BY (month_start_date)
COMMENT 'GOLD: aggregate fact. Grain: month x store x product category.'
AS
SELECT
  d.month_start_date,
  d.year_month,
  f.store_key,
  p.category,
  count(*)                                                        AS order_lines,
  count(DISTINCT f.order_id)                                      AS orders,
  sum(CASE WHEN op.is_completed THEN f.quantity        ELSE 0 END) AS units_sold,
  sum(CASE WHEN op.is_completed THEN f.gross_amount    ELSE 0 END) AS gross_amount,
  sum(CASE WHEN op.is_completed THEN f.discount_amount ELSE 0 END) AS discount_amount,
  sum(CASE WHEN op.is_completed THEN f.net_amount      ELSE 0 END) AS net_revenue,
  sum(CASE WHEN op.is_completed THEN f.cost_amount     ELSE 0 END) AS cost_amount,
  sum(CASE WHEN op.is_completed THEN f.margin_amount   ELSE 0 END) AS margin_amount,
  count_if(op.is_returned)                                        AS returned_lines,
  sum(CASE WHEN op.is_returned  THEN f.net_amount      ELSE 0 END) AS returned_net_amount,
  current_timestamp()                                             AS _loaded_at
FROM fact_sales f
JOIN dim_date          d  ON d.date_key           = f.date_key
JOIN dim_product       p  ON p.product_key        = f.product_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
GROUP BY d.month_start_date, d.year_month, f.store_key, p.category;

-- COMMAND ----------

-- DBTITLE 1,Table details (clustering, constraints)
DESCRIBE TABLE EXTENDED fact_sales;
