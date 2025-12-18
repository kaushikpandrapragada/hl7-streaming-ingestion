# Real-Time HL7 ADT Ingestion Engine 🏥⚡

### **Executive Summary**
This project demonstrates a production-grade, real-time ingestion pipeline for **HL7 v2 ADT (Admission, Discharge, Transfer)** messages using the **Databricks Lakehouse**. By leveraging **Spark Structured Streaming** and the **Medallion Architecture**, this engine transforms cryptic, legacy healthcare feeds into an actionable, high-performance "Current Hospital Census" Gold table.

---

## 🏗️ Architecture Overview
The pipeline follows the **Medallion Architecture** to ensure data quality, auditability, and clinical safety:



1.  **Landing Zone:** Raw HL7 files arrive in Cloud Object Storage (S3/ADLS/GCS).
2.  **Bronze (Raw):** **Databricks Autoloader** (`cloudFiles`) ingests raw strings incrementally. This layer preserves the original message for compliance and re-processing.
3.  **Silver (Structured):** A Spark-native parser decomposes pipe-delimited segments (`MSH`, `PID`, `PV1`) into a structured Delta table schema.
4.  **Gold (Census):** A stateful **Delta MERGE** operation maintains the "Single Source of Truth" for all currently admitted patients.

### **Data Flow**
`HL7 Feed` ➡️ `Autoloader` ➡️ `Bronze (Raw)` ➡️ `Silver (Parsed)` ➡️ `Gold (Active Census)`

---

## 🛠️ Tech Stack & Features
* **Platform:** Databricks (Unified Data Analytics Platform)
* **Storage:** Delta Lake (ACID Transactions, Time Travel, Schema Enforcement)
* **Ingestion:** Autoloader (Incremental processing with RocksDB state management)
* **Processing:** Spark Structured Streaming
* **Language:** PySpark / Spark SQL

---

## 🚀 Key Engineering Highlights
* **High-Performance Parsing:** Optimized splitting logic to handle HL7 delimiters (`|`, `^`, `&`) without the overhead of external non-distributed libraries.
* **Idempotency & State:** Uses `foreachBatch` logic to ensure that stream restarts or duplicate file arrivals do not corrupt the patient census.
* **Clinical Event Logic:** * **ADT^A01 (Admit):** Creates a new record in the Gold Census.
    * **ADT^A02 (Transfer):** Updates the patient's current unit/bed location.
    * **ADT^A03 (Discharge):** Removes the patient from the active census table (or marks as inactive).

---

## 📂 Project Structure
```text
├── notebooks/
│   ├── 01_Bronze_Ingestion.py   # Autoloader logic & Raw storage
│   ├── 02_Silver_Parsing.py     # HL7 segment extraction logic
│   └── 03_Gold_State_Merge.py   # Stateful MERGE for patient census
├── data/
│   └── sample_adt.hl7           # Sample HL7 v2 messages for testing
└── README.md                    # Project documentation
