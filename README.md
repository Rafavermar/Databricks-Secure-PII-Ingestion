# Secure PII ingestion with Databricks Free Edition

This repo shows a small, end-to-end example of how to ingest PII (Personally Identifiable Information) on **Databricks Free Edition** using:

- Unity Catalog + **Volumes** as storage
- **Auto Loader** to ingest CSV files
- Simple **column-level encryption and hashing** in PySpark
- Two SQL views: **clear** vs **masked** data

> **Disclaimer** > This is an educational PoC on Databricks Free Edition.  
> Do **not** use this project as-is for real production PII.

![DBSecurePiiFree.png](Assets/Architecture_MarinasMCP.png)
---

## 1. What you build

We use:

- Catalog: `workspace`
- Schema: `pii_demo`
- Volumes:
  - `landing_pii`     → RAW files (simulated SFTP drop zone)
  - `bronze_pii`      → encrypted Delta files
  - `checkpoints_pii` → Auto Loader checkpoints

**Data flow:**

1. You upload CSV files with PII into the `landing_pii` volume.
2. An Auto Loader streaming job reads from `landing_pii`.
3. The job applies a PII policy:
   - encrypts selected columns (`full_name`, `email`, `phone`)
   - hashes `national_id`
4. The job writes **encrypted Delta** files into `bronze_pii`.
5. Unity Catalog exposes:
   - table `customers_bronze`
   - view `v_customers_clear`   (decrypts PII – for privileged users)
   - view `v_customers_masked`  (masked PII – for normal analysts)

---

## 2. Quickstart (Free Edition)

### Step 1 – Create schema and volumes

Open `notebooks/00_setup_uc_and_volumes.py` in Databricks and **Run all**.

This will:
- use catalog `workspace`
- create schema `pii_demo`
- create Volumes: `landing_pii`, `bronze_pii`, `checkpoints_pii`

### Step 2 – Upload a sample CSV

In Databricks:  
`Data → workspace → pii_demo → Volumes → landing_pii → Upload`

Example CSV:

```csv
customer_id,full_name,email,phone,national_id,city
1,Ana Gomez,ana.gomez@example.com,+34-600000001,11111111A,Malaga
2,Luis Perez,luis.perez@example.com,+34-600000002,22222222B,Madrid
3,Maria Lopez,maria.lopez@example.com,+34-600000003,33333333C,Sevilla
```

### Step 3 – Run the Auto Loader + encryption notebook

Open `notebooks/01_autoloader_encrypt_pii.py` and:

1. **Generate a Fernet key once** (for example in a small Python cell):

   ```python
   from cryptography.fernet import Fernet
   print(Fernet.generate_key().decode("utf-8"))
   ```

2. Copy the printed value and paste it into the `ENCRYPTION_KEY` variable inside the notebook.
3. Run the `%pip install` cell once.
4. **Run all cells** to start the Auto Loader stream.

*Auto Loader will read from `landing_pii` and write encrypted Delta rows into `bronze_pii`.*

### Step 4 – Query the tables and views

From a SQL notebook or the SQL editor:

```sql
USE CATALOG workspace;
USE pii_demo;

SELECT * FROM customers_bronze;
SELECT * FROM v_customers_clear;
SELECT * FROM v_customers_masked;
```

You should see:
- **encrypted base64 strings** in `customers_bronze` for full_name, email, phone.
- **clear PII** in `v_customers_clear`.
- **masked PII** in `v_customers_masked`.

---

## 3. Going further (enterprise ideas)

This project is Free Edition-friendly on purpose. In a real enterprise deployment you would usually:

1. **Store data in external locations** on ADLS/S3 (landing/bronze/silver/gold) with private networking and customer-managed keys.
2. **Store encryption keys in Key Vault / KMS**, and read them with `dbutils.secrets.get(...)` instead of hard-coding them.
3. **Use Unity Catalog for governance**:
   - catalogs per domain.
   - grants to groups (e.g. `pii_admins`, `analysts`).
   - optional row/column-level security and dynamic masking.


![DBSecurePiiEnterprise.png](Assets/DBSecurePiiEnterprise.png)
---

## 4. Limitations

- **Educational PoC only** – not for real PII.
- **No real SFTP here:** `landing_pii` just simulates a secure file drop zone.
