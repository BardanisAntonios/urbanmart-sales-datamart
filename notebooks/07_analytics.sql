-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Analytics — answers to the business questions
-- MAGIC
-- MAGIC One query per question of the Commercial Director, using **gold tables only**. These queries are also the datasets of the AI/BI Dashboard
-- MAGIC (see [`docs/dashboard.md`](../docs/dashboard.md)).
-- MAGIC
-- MAGIC **Business rules used in every query**
-- MAGIC - **Revenue** = `net_amount` of *Completed* orders (after discount). *Returned* orders are not revenue; *Cancelled* orders are ignored.
-- MAGIC - **Gross margin** = `margin_amount` (net − cost) of *Completed* orders. `margin_pct = Σ margin / Σ net` — ratios are always computed from additive sums, never averaged.
-- MAGIC - **Return rate** = returned ÷ (completed + returned), by order lines and by value.
-- MAGIC - Guests (`customer_key = -1`) are excluded from customer rankings only.

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

-- MAGIC %md
-- MAGIC ## KPI overview (dashboard counters)

-- COMMAND ----------

-- DBTITLE 1,KPIs 2024-2025
SELECT
  sum(CASE WHEN op.is_completed THEN f.net_amount END)                                   AS net_revenue,
  sum(CASE WHEN op.is_completed THEN f.margin_amount END)                                AS gross_margin,
  round(100 * sum(CASE WHEN op.is_completed THEN f.margin_amount END)
            / sum(CASE WHEN op.is_completed THEN f.net_amount END), 1)                   AS margin_pct,
  sum(CASE WHEN op.is_completed THEN f.quantity END)                                     AS units_sold,
  count(DISTINCT CASE WHEN op.is_completed THEN f.order_id END)                          AS completed_orders,
  round(sum(CASE WHEN op.is_completed THEN f.net_amount END)
        / count(DISTINCT CASE WHEN op.is_completed THEN f.order_id END), 2)              AS avg_order_value,
  round(100 * count(DISTINCT CASE WHEN op.is_returned THEN f.order_id END)
            / count(DISTINCT CASE WHEN NOT op.is_cancelled THEN f.order_id END), 1)      AS order_return_rate_pct
FROM fact_sales f
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1 · Monthly revenue, gross margin and units sold, by region and store

-- COMMAND ----------

-- DBTITLE 1,Q1 — monthly revenue / margin / units by region and store
SELECT
  d.month_start_date,
  d.year_month,
  s.region,
  s.store_name,
  s.channel,
  sum(f.net_amount)                                        AS net_revenue,
  sum(f.margin_amount)                                     AS gross_margin,
  round(100 * sum(f.margin_amount) / sum(f.net_amount), 1) AS margin_pct,
  sum(f.quantity)                                          AS units_sold,
  count(DISTINCT f.order_id)                               AS orders
FROM fact_sales f
JOIN dim_date          d  ON d.date_key           = f.date_key
JOIN dim_store         s  ON s.store_key          = f.store_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE op.is_completed
GROUP BY d.month_start_date, d.year_month, s.region, s.store_name, s.channel
ORDER BY d.month_start_date, s.region, s.store_name;

-- COMMAND ----------

-- DBTITLE 1,Q1 — summary by region (whole period)
SELECT
  s.region,
  count(DISTINCT s.store_key)                              AS stores,
  sum(f.net_amount)                                        AS net_revenue,
  sum(f.margin_amount)                                     AS gross_margin,
  round(100 * sum(f.margin_amount) / sum(f.net_amount), 1) AS margin_pct,
  sum(f.quantity)                                          AS units_sold
FROM fact_sales f
JOIN dim_store         s  ON s.store_key          = f.store_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE op.is_completed
GROUP BY s.region
ORDER BY net_revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2 · Which product categories and brands drive the most margin?

-- COMMAND ----------

-- DBTITLE 1,Q2 — margin by category and brand
SELECT
  p.category,
  p.brand,
  sum(f.net_amount)                                        AS net_revenue,
  sum(f.margin_amount)                                     AS gross_margin,
  round(100 * sum(f.margin_amount) / sum(f.net_amount), 1) AS margin_pct,
  round(100 * sum(f.margin_amount) / sum(sum(f.margin_amount)) OVER (), 1) AS share_of_total_margin_pct,
  rank() OVER (ORDER BY sum(f.margin_amount) DESC)         AS margin_rank
FROM fact_sales f
JOIN dim_product       p  ON p.product_key        = f.product_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE op.is_completed
  AND f.product_key <> -1          -- unknown product has no cost, so no margin
GROUP BY p.category, p.brand
ORDER BY gross_margin DESC;

-- COMMAND ----------

-- DBTITLE 1,Q2 — margin by category
SELECT
  p.category,
  sum(f.net_amount)                                        AS net_revenue,
  sum(f.margin_amount)                                     AS gross_margin,
  round(100 * sum(f.margin_amount) / sum(f.net_amount), 1) AS margin_pct
FROM fact_sales f
JOIN dim_product       p  ON p.product_key        = f.product_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE op.is_completed AND f.product_key <> -1
GROUP BY p.category
ORDER BY gross_margin DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3 · Who are our top customers per segment?
-- MAGIC Top 5 customers by net revenue in each segment. Two views:
-- MAGIC - **Current segment** (`dim_customer`, SCD1) — "who are our best Loyalty Gold customers today?"
-- MAGIC - **Segment at the time of the order** (`dim_customer_scd2`, point-in-time join) — "where did revenue come from, as segments were then?"

-- COMMAND ----------

-- DBTITLE 1,Q3 — top 5 customers per current segment
SELECT segment, segment_rank, customer_id, full_name, city, net_revenue, orders
FROM (
  SELECT
    c.segment,
    c.customer_id,
    c.full_name,
    c.city,
    sum(f.net_amount)          AS net_revenue,
    count(DISTINCT f.order_id) AS orders,
    rank() OVER (PARTITION BY c.segment ORDER BY sum(f.net_amount) DESC) AS segment_rank
  FROM fact_sales f
  JOIN dim_customer      c  ON c.customer_key       = f.customer_key
  JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
  WHERE op.is_completed
    AND f.customer_key <> -1         -- exclude guest orders
  GROUP BY c.segment, c.customer_id, c.full_name, c.city
)
WHERE segment_rank <= 5
ORDER BY segment, segment_rank;

-- COMMAND ----------

-- DBTITLE 1,Q3 (point-in-time) — revenue by segment as it was on the order date vs. today
SELECT
  cv.segment                                 AS segment_at_order_time,
  c.segment                                  AS current_segment,
  count(DISTINCT c.customer_id)              AS customers,
  sum(f.net_amount)                          AS net_revenue
FROM fact_sales f
JOIN dim_customer_scd2 cv ON cv.customer_version_key = f.customer_version_key
JOIN dim_customer      c  ON c.customer_key          = f.customer_key
JOIN dim_order_profile op ON op.order_profile_key    = f.order_profile_key
WHERE op.is_completed AND f.customer_key <> -1
GROUP BY ALL
ORDER BY segment_at_order_time, current_segment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4 · Return rate by category and store type

-- COMMAND ----------

-- DBTITLE 1,Q4 — return rate by category and store type
SELECT
  p.category,
  s.store_type,
  count_if(op.is_returned)                                                     AS returned_lines,
  count(*)                                                                     AS completed_or_returned_lines,
  round(100 * count_if(op.is_returned) / count(*), 2)                          AS line_return_rate_pct,
  round(100 * sum(CASE WHEN op.is_returned THEN f.net_amount ELSE 0 END)
            / sum(f.net_amount), 2)                                            AS value_return_rate_pct
FROM fact_sales f
JOIN dim_product       p  ON p.product_key        = f.product_key
JOIN dim_store         s  ON s.store_key          = f.store_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE NOT op.is_cancelled
GROUP BY p.category, s.store_type
ORDER BY p.category, s.store_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q5 · Weekdays vs weekends, online vs physical stores
-- MAGIC A weekend has 2 days and a week has 5 weekdays, so raw totals are not comparable.
-- MAGIC `avg_revenue_per_day` normalises by the number of calendar days of each type in the period.

-- COMMAND ----------

-- DBTITLE 1,Q5 — day type x channel
WITH days AS (
  SELECT day_type, count(*) AS calendar_days
  FROM dim_date
  WHERE date_key <> -1
  GROUP BY day_type
)
SELECT
  d.day_type,
  s.channel,
  sum(f.net_amount)                                            AS net_revenue,
  count(DISTINCT f.order_id)                                   AS orders,
  round(sum(f.net_amount) / count(DISTINCT f.order_id), 2)     AS avg_order_value,
  round(sum(f.net_amount) / max(dy.calendar_days), 2)          AS avg_revenue_per_day,
  round(100 * sum(f.margin_amount) / sum(f.net_amount), 1)     AS margin_pct
FROM fact_sales f
JOIN dim_date          d  ON d.date_key           = f.date_key
JOIN dim_store         s  ON s.store_key          = f.store_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
JOIN days              dy ON dy.day_type          = d.day_type
WHERE op.is_completed
GROUP BY d.day_type, s.channel
ORDER BY s.channel, d.day_type;

-- COMMAND ----------

-- DBTITLE 1,Q5 — revenue by day of week and channel
SELECT
  d.iso_day_of_week,
  d.day_name,
  s.channel,
  sum(f.net_amount)          AS net_revenue,
  count(DISTINCT f.order_id) AS orders
FROM fact_sales f
JOIN dim_date          d  ON d.date_key           = f.date_key
JOIN dim_store         s  ON s.store_key          = f.store_key
JOIN dim_order_profile op ON op.order_profile_key = f.order_profile_key
WHERE op.is_completed
GROUP BY d.iso_day_of_week, d.day_name, s.channel
ORDER BY d.iso_day_of_week, s.channel;