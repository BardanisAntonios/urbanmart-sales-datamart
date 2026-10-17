-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup — UrbanMart Sales Data Mart
-- MAGIC
-- MAGIC Creates the Unity Catalog objects used by every other notebook of the pipeline.
-- MAGIC
-- MAGIC | Object | Type | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | `<catalog>.<bronze_schema>` | Schema | Raw CSV data, all columns STRING + lineage |
-- MAGIC | `<catalog>.<bronze_schema>.landing` | Volume | Drop zone for the 6 source CSV files |
-- MAGIC | `<catalog>.<silver_schema>` | Schema | Cleansed, typed, de-duplicated data + `etl_rejects` |
-- MAGIC | `<catalog>.<gold_schema>` | Schema | Star schema (`dim_*`, `fact_sales`) |
-- MAGIC
-- MAGIC **Author:** Antonios Bardanis · EY Data Engineering Bootcamp — Capstone
-- MAGIC
-- MAGIC **Idempotent:** every statement uses `IF NOT EXISTS`, so the notebook can be re-run safely.

-- COMMAND ----------

-- DBTITLE 1,Parameters
-- One widget per object name. Every notebook of the pipeline declares the same widgets,
-- and the Lakeflow Job passes the same values to all tasks (job parameters).
CREATE WIDGET TEXT catalog       DEFAULT 'urbanmart';
CREATE WIDGET TEXT bronze_schema DEFAULT 'bronze';
CREATE WIDGET TEXT silver_schema DEFAULT 'silver';
CREATE WIDGET TEXT gold_schema   DEFAULT 'gold';

-- COMMAND ----------

-- DBTITLE 1,Catalog (run once, only if you own the metastore / Free Edition)
-- Creating a catalog needs the CREATE CATALOG privilege. On Databricks Free Edition you have it.
-- In a shared workspace use a catalog you already own and put its name in the `catalog` widget.
-- Uncomment the next line the first time only:
-- CREATE CATALOG IF NOT EXISTS urbanmart COMMENT 'UrbanMart retail analytics (capstone).';

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- DBTITLE 1,Medallion schemas
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:bronze_schema)
  COMMENT 'BRONZE: raw UrbanMart CSV extracts, all columns STRING, plus _source_file and _ingested_at.';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:silver_schema)
  COMMENT 'SILVER: cleansed, typed and de-duplicated UrbanMart data. Bad rows go to etl_rejects.';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:gold_schema)
  COMMENT 'GOLD: UrbanMart sales star schema (dim_date, dim_customer, dim_product, dim_store, fact_sales).';

-- COMMAND ----------

-- DBTITLE 1,Landing volume
-- A managed volume inside the bronze schema. Upload the 6 CSV files here:
-- Catalog Explorer -> <catalog> -> <bronze_schema> -> Volumes -> landing -> "Upload to this volume".
USE SCHEMA IDENTIFIER(:bronze_schema);

CREATE VOLUME IF NOT EXISTS landing
  COMMENT 'Landing zone for the UrbanMart CSV extracts (customers, customer_changes, products, stores, orders, order_items).';

-- COMMAND ----------

-- DBTITLE 1,Check the objects
SHOW SCHEMAS;

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check the upload
-- MAGIC After uploading the files, the cell below must list **6 CSV files**.
-- MAGIC (Equivalent to `LIST '/Volumes/urbanmart/bronze/landing/'` for the default widget values.)

-- COMMAND ----------

-- DBTITLE 1,Files in the landing volume (expected: 6)
-- MAGIC %python
-- MAGIC landing = f"/Volumes/{dbutils.widgets.get('catalog')}/{dbutils.widgets.get('bronze_schema')}/landing/"
-- MAGIC files = dbutils.fs.ls(landing)
-- MAGIC display(spark.createDataFrame([(f.name, f.size) for f in files], "file_name STRING, size_bytes BIGINT"))
-- MAGIC print(f"{len(files)} file(s) in {landing}")
