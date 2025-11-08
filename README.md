# 🧩 RLS_GCP_BigQuery

In this repository, you can find an automated **Row Level Security (RLS)** management process for **BigQuery**, fully orchestrated through **Airflow (Cloud Composer)** or with Cloud Fuction, and powered by configuration files stored in **Google Cloud Storage (GCS)**.

---

## 🧠 Project Description

This project automates the application of **RLS policies** in BigQuery.  
The process reads the **RLS rule definitions** from a file stored in GCS, loads them into a BigQuery table, removes any old policies, and creates new ones dynamically — ensuring full synchronization between the defined rules and the active RLS configuration.  

It also includes **auditing** and **change detection** between rule versions.

---

## 🗃️ Project Files

1. **RLS_DAG.py** – Airflow DAG ready for deployment on **Composer (GCP)**.  
2. **requirements.txt** – List of dependencies required by the DAG.  
3. **rls_rules.csv** – Example input file containing RLS rules (source definition).  
4. **Deployment_Config_Composer** – Example of environment variables and Airflow connections required to deploy the process.
5. **main.py** - Coud Fuction ready to implemented in GCP.
---

## ⚙️ Process Overview

1. 📥 Validate and convert input file (Excel → CSV).  
2. 🗃️ Load the configuration into a **BigQuery table**.  
3. 🧹 Remove existing **RLS policies**.  
4. 🧩 Dynamically generate new RLS policies from rules.  
5. 🧾 Apply policies and store **audit logs**.  
6. 🔍 Detect and store **differences** between policy versions.

---

## 🔧 Declarative Section

Main variables used in the DAG:

```python
PROJECT_ID = "test-x-xxxxx"
DATASET = "test_xxx"
TABLA = "nombre de la tabla que contiene las reglas de RLS"
TABLE_REGLAS = f"{PROJECT_ID}.{DATASET}.{TABLA}"
TABLE_AUDITORIA = f"{PROJECT_ID}.{DATASET}.nombre_tabla_auditoria"
BUCKET = "us-central1-xxx-xxxxxx-bucket"
SOURCE_FILE = "data/nombre_archivo_reglas_rls.csv"
```

These variables make it easy to adapt the DAG across different environments (dev/prod) and only need to be configured once.

---

## 📦 Helper Functions

### 🧩 `sanitize_identifier(s: str) -> str`

Converts any string into a valid BigQuery identifier by replacing unsupported characters with underscores (`_`).  
Used to create compliant `policy_name` values.

---

## 📄 Step 1: Validate and Convert Input File

**Function:** `verificar_y_convertir(bucket, source_object, **kwargs)`  
**Objective:** Validate the format of the rule file in GCS and convert Excel files to CSV automatically.

**Steps:**
- Downloads the file from GCS using `GCSHook`.  
- Detects file extension (`.csv`, `.xlsx`, `.xls`).  
- If it’s Excel, converts to CSV via pandas and reuploads to GCS.  
- Publishes the CSV filename to XCom for later use.

---

## 🧹 Step 2: Delete Existing RLS Policies

**Function:** `eliminar_rls_existentes(**kwargs)`  
**Objective:** Remove all current RLS policies from the tables listed in `TABLE_REGLAS` to prevent conflicts.

**Steps:**
- Queries all combinations of **project**, **dataset**, and **table** from `TABLE_REGLAS`.  
- Lists active policies from `INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`.  
- Executes `DROP ROW ACCESS POLICY` for each one.  
- Logs success and errors for traceability.

---

## 🧱 Step 3: Generate Dynamic RLS Policies

**Function:** `generar_rls_dinamico(**kwargs)`  
**Objective:** Dynamically build RLS policies based on the rules stored in `TABLE_REGLAS`.

**Steps:**
- Reads rules from BigQuery and groups by `(Project, Dataset, Table, User)`.  
- Creates a new `batch_id` (incremental version).  
- Builds filtering conditions combining rule columns (`Dimension_X`, `Condicion_X`).  
- Generates SQL statements like:

```sql
CREATE OR REPLACE ROW ACCESS POLICY rls_test_RLS_ventas_user_juan
ON `test-1-426619.test_RLS.ventas`
GRANT TO ('user:juan@empresa.com')
FILTER USING (pais = 'AR' OR pais = 'BR');
```

- Stores:
  - `queries`: list of SQL statements.
  - `auditoria`: metadata logs (user, table, hash, date, etc.) in XCom.

---

## 🚀 Step 4: Apply RLS Policies and Audit

**Function:** `aplicar_rls_dinamico(**context)`  
**Objective:** Execute generated SQL statements and insert audit data.

**Steps:**
- Retrieves `queries` and `auditoria` from XCom.  
- Runs SQL statements via BigQuery client.  
- Inserts audit records into `TABLE_AUDITORIA` (mode `WRITE_APPEND`).  

**Audit fields:**
- Project, dataset, table, user, condition.  
- Execution date, hash ID, batch ID.

---

## 🔍 Step 5: Detect and Record Changes Between Versions

**Function:** `detectar_cambios_rls(**kwargs)`  
**Objective:** Compare the current batch with the previous one to identify added, removed, or unchanged policies.

**Steps:**
- Ensures the table `auditoria_rls_cambios` exists.  
- Calculates current and previous batch IDs.  
- Performs a `FULL OUTER JOIN` between both batches using `hash_id`.  
- Categorizes results as:
  - `NUEVA` (new)
  - `ELIMINADA` (removed)
  - `EXISTENTE` (unchanged)
- Saves results to `auditoria_rls_cambios` (`WRITE_TRUNCATE`).

---

## 🔁 DAG Flow

```text
verificar_archivo
      ↓
gcs_to_bq
      ↓
eliminar_rls_existentes
      ↓
generar_rls_dinamico
      ↓
aplicar_rls_dinamico
      ↓
detectar_cambios_rls
```

**Sequence Summary:**
1. Validate and convert source file.  
2. Load CSV into BigQuery (`reglas_rls`).  
3. Clean existing RLS policies.  
4. Dynamically generate SQL and metadata.  
5. Apply new RLS policies and record audits.  
6. Detect changes between versions.

---

## 🧾 Tables Used

| Table | Description | Write Mode |
|--------|--------------|------------|
| `reglas_rls` | Contains source RLS rules from GCS | `WRITE_TRUNCATE` |
| `auditoria_rls` | Logs applied policy details | `WRITE_APPEND` |
| `auditoria_rls_cambios` | Stores detected differences between batches | `WRITE_TRUNCATE` |

---

## 🧩 Technologies Used

☁️ **Google Cloud Platform (GCP)**  
- BigQuery  
- Cloud Storage  
- Cloud Composer (Airflow)

🐍 **Python**  
- pandas  
- google-cloud-bigquery  
- apache-airflow

---

## 📬 Contact

Created by **Emiliano Rigueiro**  
💼 Data Engineer
