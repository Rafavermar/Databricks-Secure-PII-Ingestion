# Secure PII ingestion with Databricks Free Edition

This repo shows a minimal end-to-end pattern to ingest PII data on **Databricks Free Edition** using:

- Unity Catalog + Volumes as storage
- Auto Loader to ingest CSV files
- Simple **column-level encryption and hashing** in PySpark
- Clear vs masked SQL views for consumers

> **Disclaimer** > This is an educational PoC on Databricks Free Edition.  
> Do **not** use this project as-is for real production PII.

---

## 1. Architecture (Free Edition)

**Free Edition constraints:**
- Storage is **managed by Databricks**.
- We use **Unity Catalog** with:
  - Catalog: `main` (default)
  - Schema: `pii_demo`
  - Volumes:
    - `landing_pii`     → RAW files (simulates SFTP drop zone)
    - `bronze_pii`      → encrypted Delta files
    - `checkpoints_pii` → Auto Loader checkpoints

**Data flow:**
1. You upload CSV files with PII into the `landing_pii` volume.
2. An Auto Loader streaming job reads from `landing_pii`.
3. The job applies a PII policy:
   - encrypts selected columns (e.g. `full_name`, `email`, `phone`),
   - hashes others (e.g. `national_id`),
   - optionally drops unwanted columns.
4. The job writes **encrypted Delta** files into `bronze_pii`.
5. Unity Catalog exposes:
   - table `customers_bronze`
   - view `v_customers_clear`   (decrypts PII – for privileged users)
   - view `v_customers_masked`  (masked PII – for normal analysts)

---

## 2. How to run (Free Edition)

### 1. Create UC objects and volumes
Open the notebook `notebooks/00_setup_uc_and_volumes.py` in Databricks and run all cells.  
This will:
- use catalog `main`
- create schema `pii_demo`
- create volumes `landing_pii`, `bronze_pii`, `checkpoints_pii`

### 2. Upload a sample CSV
In Databricks, go to:  
`Data` → `main` → `pii_demo` → `Volumes` → `landing_pii` → **Upload**

Upload a small CSV file like:

```csv
customer_id,full_name,email,phone,national_id,city
1,Ana Gómez,ana.gomez@example.com,+34-600000001,11111111A,Málaga
2,Luis Pérez,luis.perez@example.com,+34-600000002,22222222B,Madrid
```

### 3. Run the Auto Loader + encryption pipeline
Open `notebooks/01_autoloader_encrypt_pii.py` and:
1. set a proper `ENCRYPTION_KEY` (Fernet key string),
2. run the `%pip install` cell once,
3. run all cells to start the streaming query.

Auto Loader will ingest from `landing_pii` and write encrypted Delta rows into `bronze_pii`.

### 4. Query the tables and views
From a SQL notebook or the SQL editor:

```sql
USE CATALOG main;
USE pii_demo;

SELECT * FROM customers_bronze;
SELECT * FROM v_customers_clear;
SELECT * FROM v_customers_masked;
```

---

## 3. From Free Edition to an enterprise deployment

This PoC focuses on Databricks Free Edition. In a real enterprise deployment, you would typically change the following:

### 3.1. Storage

| Free Edition | Enterprise |
| :--- | :--- |
| Uses Databricks-managed storage and UC Volumes. | Use **external locations** mapped to Azure Data Lake Storage (ADLS) or Amazon S3. |
| | Separated zones: landing, bronze, silver, gold. |
| | Add private networking (Private Link / VNet / VPC), customer-managed keys and storage policies. |

### 3.2. Secrets and keys

| Free Edition | Enterprise |
| :--- | :--- |
| This PoC uses a hard-coded Fernet key in the notebook (only for learning). | Store keys in a **secure secret manager** (e.g. Azure Key Vault, AWS KMS). |
| | Read them with Databricks secret scopes, e.g.:<br>`ENCRYPTION_KEY = dbutils.secrets.get("pii-scope", "fernet-key")` |
| | Implement key rotation and audit. |

### 3.3. Governance and access

| Free Edition | Enterprise |
| :--- | :--- |
| Uses two views:<br>- `v_customers_clear` (decrypts columns)<br>- `v_customers_masked` (shows masked output). | Use **Unity Catalog**:<br>- catalogs per domain (e.g. pii, sales, logs),<br>- grants to groups (e.g. pii_admins, analysts). |
| | Optional row- and column-level security, dynamic masking. |
| | Log access, and integrate with identity provider (AAD/IdP). |

---

## 4. Limitations

- **Educational PoC only** – not for real PII.
- **No real SFTP here:** `landing_pii` simply simulates a secure file drop zone. In a production setup, SFTP or another ingestion system would drop files into landing.
