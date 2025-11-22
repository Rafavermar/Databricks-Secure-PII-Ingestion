# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Auto Loader + column-level encryption for PII
# MAGIC
# MAGIC This notebook:
# MAGIC
# MAGIC - reads CSV files from the `landing_pii` Volume using Auto Loader
# MAGIC - applies a simple PII policy:
# MAGIC   - encrypts `full_name`, `email`, `phone` using Fernet
# MAGIC   - hashes `national_id` with SHA-256
# MAGIC - writes an **encrypted Delta** table into `bronze_pii`
# MAGIC - creates a Delta table and two views:
# MAGIC   - `customers_bronze`      (encrypted columns)
# MAGIC   - `v_customers_clear`     (decrypts columns)
# MAGIC   - `v_customers_masked`    (masked output for normal analysts)
# MAGIC
# MAGIC > NOTE[enterprise]:
# MAGIC > In a real deployment you would:
# MAGIC > - store the encryption key in a secret manager (Key Vault / KMS),
# MAGIC > - use external locations pointing to ADLS/S3,
# MAGIC > - secure views with proper grants and group-based access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies (run once per cluster)
# MAGIC
# MAGIC We use the `cryptography` package for Fernet encryption.
# MAGIC
# MAGIC Run this cell once when you start your cluster.

# COMMAND ----------

# MAGIC %pip install cryptography

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration: paths and encryption key
# MAGIC
# MAGIC - Paths point to Unity Catalog Volumes created in `00_setup_uc_and_volumes`.
# MAGIC - `ENCRYPTION_KEY` must be set to a valid Fernet key string.
# MAGIC
# MAGIC > NOTE[enterprise]:
# MAGIC > - Do **not** hard-code keys.
# MAGIC > - Fetch them from a secret manager (Key Vault / KMS) via `dbutils.secrets`.

# COMMAND ----------

from typing import Optional

import pandas as pd
from cryptography.fernet import Fernet
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, udf, pandas_udf
from pyspark.sql.types import StringType

# Paths in Unity Catalog Volumes (Free Edition)
RAW_PATH = "/Volumes/workspace/pii_demo/landing_pii"
BRONZE_PATH = "/Volumes/workspace/pii_demo/bronze_pii"
CHECKPOINT_PATH = (
    "/Volumes/workspace/pii_demo/checkpoints_pii/autoloader_pii"
)

# NOTE[enterprise]:
#   - In production, you would use external locations:
#   - RAW_PATH    = "abfss://landing@<storage>.dfs.core.windows.net/pii_demo/raw"
#   - BRONZE_PATH = "abfss://bronze@<storage>.dfs.core.windows.net/pii_demo/bronze"


# NOTE[enterprise]:
#   - Never hard-code encryption keys.
#   - Example real-world pattern:
#   -
#   # ENCRYPTION_KEY = dbutils.secrets.get("pii-scope", "fernet-key")
#   -
#   - Secret would live in Key Vault / KMS and be rotated regularly.


# PoC / Free Edition:
#   - Fernet key is hard-coded for learning purposes only.
#   - Generate a Fernet key **once** and paste it here:
#   -
#   # from cryptography.fernet import Fernet
#   # print(Fernet.generate_key().decode("utf-8"))
ENCRYPTION_KEY = "CHANGE_ME_WITH_A_REAL_FERNET_KEY"

if ENCRYPTION_KEY == "CHANGE_ME_WITH_A_REAL_FERNET_KEY":
    raise ValueError(
        "Set ENCRYPTION_KEY to a real Fernet key string before running."
    )

FERNET = Fernet(ENCRYPTION_KEY.encode("utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PII policy functions
# MAGIC
# MAGIC Here we define:
# MAGIC
# MAGIC - which columns to encrypt / hash / drop
# MAGIC - the `encrypt_value` function and UDF
# MAGIC - the `apply_pii_policy` transformation
# MAGIC - the `decrypt_value` UDF used in SQL views

# COMMAND ----------

ENCRYPT_COLUMNS = ["full_name", "email", "phone"]
HASH_COLUMNS = ["national_id"]
DROP_COLUMNS: list[str] = []


def encrypt_value(value: Optional[str]) -> Optional[str]:
    """Encrypt a string using Fernet (base64 ciphertext)."""
    if value is None:
        return None
    data = value.encode("utf-8")
    token = FERNET.encrypt(data)
    return token.decode("utf-8")


encrypt_udf = udf(encrypt_value, StringType())


def apply_pii_policy(df: DataFrame) -> DataFrame:
    """Apply encryption, hashing and dropping rules to a DataFrame."""
    result = df

    # Encrypt selected columns
    for name in ENCRYPT_COLUMNS:
        if name in result.columns:
            result = result.withColumn(name, encrypt_udf(col(name)))

    # Hash selected columns (irreversible)
    for name in HASH_COLUMNS:
        if name in result.columns:
            result = result.withColumn(
                name,
                sha2(col(name).cast("string"), 256),
            )

    # Drop unwanted columns (if any)
    for name in DROP_COLUMNS:
        if name in result.columns:
            result = result.drop(name)

    return result


@pandas_udf(StringType())
def decrypt_value(col_series: pd.Series) -> pd.Series:
    """Decrypt a batch of Fernet-encrypted strings."""
    def _decrypt_one(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        token = value.encode("utf-8")
        data = FERNET.decrypt(token)
        return data.decode("utf-8")

    return col_series.apply(_decrypt_one)


# Register the decrypt UDF for SQL usage
spark.udf.register("decrypt_value", decrypt_value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Start Auto Loader stream
# MAGIC
# MAGIC - Reads CSV files from `RAW_PATH` (`landing_pii` Volume)
# MAGIC - Applies the PII policy in-memory
# MAGIC - Writes encrypted Delta rows into `BRONZE_PATH` (`bronze_pii` Volume)

# COMMAND ----------

source_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(RAW_PATH)
)

pii_safe_df = apply_pii_policy(source_df)

query = (
    pii_safe_df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start(BRONZE_PATH)
)

# You can inspect the stream with:
# query.status
# query.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Delta table and views on top of the encrypted data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE pii_demo;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers_bronze
# MAGIC USING DELTA
# MAGIC LOCATION '/Volumes/workspace/pii_demo/bronze_pii';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View with decrypted columns (for privileged users only)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_customers_clear AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   decrypt_value(full_name) AS full_name,
# MAGIC   decrypt_value(email)     AS email,
# MAGIC   decrypt_value(phone)     AS phone,
# MAGIC   national_id,
# MAGIC   city
# MAGIC FROM customers_bronze;
# MAGIC
# MAGIC -- NOTE[enterprise]:
# MAGIC --   In a real setup you would restrict access, for example:
# MAGIC --   GRANT SELECT ON VIEW v_customers_clear TO `pii_admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masked view for normal analysts
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_customers_masked AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   concat(substr(decrypt_value(full_name), 1, 1), '***')
# MAGIC     AS full_name_masked,
# MAGIC   concat('***', substr(decrypt_value(email), -10, 10))
# MAGIC     AS email_masked,
# MAGIC   concat('***', substr(decrypt_value(phone), -4, 4))
# MAGIC     AS phone_masked,
# MAGIC   national_id,
# MAGIC   city
# MAGIC FROM customers_bronze;
# MAGIC
# MAGIC -- NOTE[enterprise]:
# MAGIC --   You would typically grant this masked view to a wider group:
# MAGIC --   GRANT SELECT ON VIEW v_customers_masked TO `analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quick manual checks
# MAGIC
# MAGIC From a SQL notebook or the SQL editor you can now:
# MAGIC
# MAGIC ```sql
# MAGIC USE CATALOG main;
# MAGIC USE pii_demo;
# MAGIC
# MAGIC SELECT * FROM customers_bronze;
# MAGIC SELECT * FROM v_customers_clear;
# MAGIC SELECT * FROM v_customers_masked;
# MAGIC ```
# MAGIC
# MAGIC You should see:
# MAGIC
# MAGIC - encrypted base64 strings in `customers_bronze` for `full_name`, `email`, `phone`
# MAGIC - human-readable PII in `v_customers_clear`
# MAGIC - masked PII in `v_customers_masked`.
