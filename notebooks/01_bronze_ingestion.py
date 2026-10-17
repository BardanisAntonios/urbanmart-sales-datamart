# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Raw ingestion
# MAGIC
# MAGIC Loads each CSV file of the landing volume into **one bronze Delta table**, driven by a small metadata list (one entry per file).
# MAGIC
# MAGIC **Bronze rules**
# MAGIC - Every business column is kept as **STRING**, exactly as it is in the file (no trimming, no casting). Bad values must still be visible here.
# MAGIC - Two lineage columns are added: `_source_file` (from `_metadata.file_path`) and `_ingested_at`.
# MAGIC - `CREATE OR REPLACE TABLE … AS SELECT … FROM read_files(…)` makes the load **idempotent**: a re-run replaces the table instead of
# MAGIC   appending duplicates, and every load becomes a new Delta version (see `DESCRIBE HISTORY` in notebook 08).
# MAGIC - Columns are listed explicitly, so an unexpected extra column in a future file cannot silently change the table.
# MAGIC
# MAGIC *Why Python here?* The statements are pure Databricks SQL; Python only builds the volume path from the widgets.
# MAGIC Named parameter markers inside `read_files()` / CTAS are not supported on every runtime, an f-string is.
# MAGIC
# MAGIC | Source file | Bronze table | Expected rows |
# MAGIC |---|---|---|
# MAGIC | customers.csv | `customers` | 508 |
# MAGIC | customer_changes.csv | `customer_changes` | 25 |
# MAGIC | products.csv | `products` | 76 |
# MAGIC | stores.csv | `stores` | 13 |
# MAGIC | orders.csv | `orders` | 8,000 |
# MAGIC | order_items.csv | `order_items` | 24,111 |

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "urbanmart")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")

catalog       = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
landing_path  = f"/Volumes/{catalog}/{bronze_schema}/landing"
print(f"Target schema : {catalog}.{bronze_schema}\nLanding volume: {landing_path}")

# COMMAND ----------

# DBTITLE 1,Source metadata (file -> table, columns, description)
SOURCES = [
    {"table": "customers",        "file": "customers.csv",        "expected_rows": 508,
     "columns": ["customer_id", "first_name", "last_name", "email", "gender", "birth_date", "city", "region", "segment", "signup_date"],
     "comment": "raw customers.csv (customer master)"},
    {"table": "customer_changes", "file": "customer_changes.csv", "expected_rows": 25,
     "columns": ["customer_id", "new_city", "new_region", "new_segment", "effective_date"],
     "comment": "raw customer_changes.csv (city / region / segment change events, input for SCD Type 2)"},
    {"table": "products",         "file": "products.csv",         "expected_rows": 76,
     "columns": ["product_id", "product_name", "category", "subcategory", "brand", "unit_cost", "list_price", "is_active"],
     "comment": "raw products.csv (product catalogue with cost and list price)"},
    {"table": "stores",           "file": "stores.csv",           "expected_rows": 13,
     "columns": ["store_id", "store_name", "store_type", "city", "region", "open_date", "size_sqm"],
     "comment": "raw stores.csv (12 physical stores + 1 online store)"},
    {"table": "orders",           "file": "orders.csv",           "expected_rows": 8000,
     "columns": ["order_id", "order_ts", "customer_id", "store_id", "payment_method", "order_status"],
     "comment": "raw orders.csv (order header; grain = one order)"},
    {"table": "order_items",      "file": "order_items.csv",      "expected_rows": 24111,
     "columns": ["order_line_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"],
     "comment": "raw order_items.csv (order lines; grain = one order line)"},
]

# COMMAND ----------

# DBTITLE 1,Check the landing volume
files_in_volume = {f.name for f in dbutils.fs.ls(landing_path)}
missing = [s["file"] for s in SOURCES if s["file"] not in files_in_volume]
assert not missing, f"Missing files in {landing_path}: {missing}. Upload them first (see notebook 00)."
print("All 6 source files found:", sorted(files_in_volume))

# COMMAND ----------

# DBTITLE 1,Load every file into its bronze table
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{bronze_schema}`")

for src in SOURCES:
    columns_sql = ",\n  ".join(f"`{c}`" for c in src["columns"])
    spark.sql(f"""
      CREATE OR REPLACE TABLE {src['table']}
      COMMENT 'BRONZE: {src['comment']}. All columns STRING as in the file, plus _source_file and _ingested_at.'
      AS
      SELECT
        {columns_sql},
        _metadata.file_path AS _source_file,
        current_timestamp() AS _ingested_at
      FROM read_files(
        '{landing_path}/{src['file']}',
        format           => 'csv',
        header           => true,
        inferColumnTypes => false
      )
    """)
    print(f"loaded {catalog}.{bronze_schema}.{src['table']}")

# COMMAND ----------

# DBTITLE 1,Row counts vs. specification (fails the run if a table is empty)
from pyspark.sql import Row

results = []
for src in SOURCES:
    actual = spark.table(src["table"]).count()
    results.append(Row(table_name=src["table"], expected_rows=src["expected_rows"],
                       actual_rows=actual, matches_spec=(actual == src["expected_rows"])))
display(spark.createDataFrame(results))

empty = [r.table_name for r in results if r.actual_rows == 0]
assert not empty, f"Empty bronze tables: {empty}"

# COMMAND ----------

# DBTITLE 1,All bronze columns are STRING
display(spark.sql(f"""
  SELECT table_name, column_name, data_type
  FROM `{catalog}`.information_schema.columns
  WHERE table_schema = '{bronze_schema}'
    AND table_name IN ({", ".join(f"'{s['table']}'" for s in SOURCES)})
    AND data_type <> 'STRING'
    AND column_name <> '_ingested_at'
"""))
