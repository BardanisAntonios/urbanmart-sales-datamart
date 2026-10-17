# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Demo — Delta history, time travel, idempotency & enforced constraints
# MAGIC
# MAGIC Used in the 10-minute demo. **Not part of the Lakeflow Job.** Run it after at least two runs of the pipeline.
# MAGIC
# MAGIC | Part | Shows |
# MAGIC |---|---|
# MAGIC | 1 | `DESCRIBE HISTORY` — every load is a Delta version with operation metrics |
# MAGIC | 2 | Idempotency — the 2nd `MERGE` into `fact_sales` inserted / updated / deleted **0** rows |
# MAGIC | 3 | Time travel — change a row, compare `VERSION AS OF` with the current table, then `RESTORE` |
# MAGIC | 4 | `CHECK` constraints are enforced — a bad row is refused |
# MAGIC | 5 | The validation gate fails on bad data — `assert_true` raises an error |

# COMMAND ----------

dbutils.widgets.text("catalog", "urbanmart")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")

catalog = dbutils.widgets.get("catalog")
gold    = f"{catalog}.{dbutils.widgets.get('gold_schema')}"
bronze  = f"{catalog}.{dbutils.widgets.get('bronze_schema')}"
print(f"gold schema   = {gold}\nbronze schema = {bronze}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table history

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {gold}.fact_sales")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# Bronze tables are rebuilt with CREATE OR REPLACE: one version per load.
display(spark.sql(f"DESCRIBE HISTORY {bronze}.orders")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotency — what did each MERGE into `fact_sales` change?
# MAGIC The first load inserts all lines; every later run of the unchanged pipeline inserts, updates and deletes **0** rows.

# COMMAND ----------

from pyspark.sql import functions as F

merges = (spark.sql(f"DESCRIBE HISTORY {gold}.fact_sales")
          .where("operation = 'MERGE'")
          .select("version", "timestamp",
                  F.col("operationMetrics.numTargetRowsInserted").alias("rows_inserted"),
                  F.col("operationMetrics.numTargetRowsUpdated").alias("rows_updated"),
                  F.col("operationMetrics.numTargetRowsDeleted").alias("rows_deleted"))
          .orderBy("version"))
display(merges)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel on `dim_customer`
# MAGIC Simulate a manual change, see exactly what changed between two versions, then roll back with `RESTORE`.

# COMMAND ----------

before = spark.sql(f"DESCRIBE HISTORY {gold}.dim_customer").agg(F.max("version")).first()[0]
print(f"dim_customer version before the change: {before}")

spark.sql(f"""
  UPDATE {gold}.dim_customer
  SET segment = 'Loyalty Gold', _updated_at = current_timestamp()
  WHERE customer_id = 'C0001'
""")

after = spark.sql(f"DESCRIBE HISTORY {gold}.dim_customer").agg(F.max("version")).first()[0]
print(f"dim_customer version after the change:  {after}")

# COMMAND ----------

# Row as it was (VERSION AS OF) vs. row now
display(spark.sql(f"""
  SELECT 'version {before}' AS version, customer_id, segment FROM {gold}.dim_customer VERSION AS OF {before} WHERE customer_id = 'C0001'
  UNION ALL
  SELECT 'version {after}',  customer_id, segment FROM {gold}.dim_customer VERSION AS OF {after}  WHERE customer_id = 'C0001'
"""))

# COMMAND ----------

# Every row that differs between the two versions
display(spark.sql(f"""
  SELECT * EXCEPT (_updated_at) FROM {gold}.dim_customer VERSION AS OF {after}
  EXCEPT
  SELECT * EXCEPT (_updated_at) FROM {gold}.dim_customer VERSION AS OF {before}
"""))

# COMMAND ----------

# Roll back the manual change
display(spark.sql(f"RESTORE TABLE {gold}.dim_customer TO VERSION AS OF {before}"))
display(spark.sql(f"SELECT customer_id, segment FROM {gold}.dim_customer WHERE customer_id = 'C0001'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CHECK constraints are enforced
# MAGIC `fact_sales` has `CHECK (quantity > 0)`. Delta refuses the insert below — the table is not changed.

# COMMAND ----------

try:
    spark.sql(f"""
      INSERT INTO {gold}.fact_sales
      SELECT 'DEMO-BAD-LINE', 'DEMO', -1, -1, -1, -1, -1, -1, current_timestamp(),
             -5, 10.00, 0, NULL, -50.00, 0.00, -50.00, NULL, NULL, 'demo', current_timestamp()
    """)
    print("Unexpected: the row was accepted")
except Exception as e:
    print("Refused as expected ->", str(e).splitlines()[0][:300])

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {gold}.fact_sales").where("key LIKE 'delta.constraints%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The validation gate fails on bad data
# MAGIC Same orphan-key test as V07 in notebook 06, run against a fake fact row whose `product_key` (-999) does not exist.

# COMMAND ----------

try:
    spark.sql(f"""
      SELECT assert_true(count(*) = 0, 'Validation V07 failed: orphan fact_sales.product_key') AS gate
      FROM (SELECT CAST(-999 AS BIGINT) AS product_key) AS bad_fact
      LEFT ANTI JOIN {gold}.dim_product d ON d.product_key = bad_fact.product_key
    """).collect()
    print("Unexpected: the check passed")
except Exception as e:
    print("Gate failed as expected ->", str(e).splitlines()[0][:300])
