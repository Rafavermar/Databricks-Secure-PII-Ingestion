# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup Unity Catalog schema and Volumes
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - uses the default Unity Catalog (`main`)
# MAGIC - creates schema `pii_demo`
# MAGIC - creates three Volumes:
# MAGIC   - `landing_pii`      (RAW files, simulates SFTP drop zone)
# MAGIC   - `bronze_pii`       (encrypted Delta files)
# MAGIC   - `checkpoints_pii`  (Auto Loader checkpoints)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect available catalogs
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the default catalog.
# MAGIC -- NOTE: if `main` does not exist in your workspace,
# MAGIC --       replace it with the default catalog you see above.
# MAGIC USE CATALOG workspace;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS pii_demo;
# MAGIC USE pii_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Volumes for landing, bronze and checkpoints.
# MAGIC -- These live on Databricks-managed storage in Free Edition.
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS landing_pii
# MAGIC   COMMENT 'Landing (raw) PII files - simulates SFTP drop zone';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS bronze_pii
# MAGIC   COMMENT 'Bronze encrypted PII files';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoints_pii
# MAGIC   COMMENT 'Checkpoints for Auto Loader streams';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. Go to **Catalog → workspace → pii_demo → Volumes**.
# MAGIC 2. Open `landing_pii` and **upload a sample CSV** with PII columns.
# MAGIC 3. Then open and run `01_autoloader_encrypt_pii.py`.
