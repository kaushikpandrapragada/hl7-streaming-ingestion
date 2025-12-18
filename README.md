# hl7-streaming-ingestion
HL7 Autoloading using Databricks Autoloader
Real-Time HL7 ADT Ingestion Engine 🏥⚡
Executive Summary

This project demonstrates a production-grade, real-time ingestion pipeline for HL7 v2 ADT (Admission, Discharge, Transfer) messages using the Databricks Lakehouse. By leveraging Spark Structured Streaming and the Medallion Architecture, this engine transforms cryptic, legacy healthcare feeds into an actionable, high-performance "Current Hospital Census" Gold table.
🏗️ Architecture Overview

The pipeline is built on the Medallion Architecture to ensure data quality and lineage:

    Landing Zone: Raw HL7 files arrive in Cloud Object Storage (S3/ADLS).

    Bronze (Raw): Databricks Autoloader (cloudFiles) ingests raw strings incrementally with schema evolution support.

    Silver (Structured): A Spark-native parser decomposes pipe-delimited segments (MSH, PID, PV1) into a structured schema.

    Gold (Census): A stateful Delta MERGE operation maintains the current occupancy of the hospital, handling admits, transfers, and discharges.

Simple Logic Flow

Raw HL7 Feed ➡️ Autoloader ➡️ Bronze Delta ➡️ Spark Transformation ➡️ Silver Delta ➡️ Stateful Merge ➡️ Gold Census Table
🛠️ Tech Stack & Features

    Engine: Apache Spark (Databricks Runtime)

    Storage: Delta Lake (ACID compliant, Time Travel)

    Ingestion: Autoloader (Incremental, cost-efficient processing)

    Processing: Structured Streaming (Low-latency clinical updates)

    Language: PySpark / Spark SQL

🚀 Key Engineering Highlights

    Idempotency: The pipeline uses foreachBatch with MERGE logic to ensure that late-arriving data or stream restarts do not result in duplicate records.

    Complex Parsing: Custom handling of HL7 delimiters (|, ^) without relying on heavy external dependencies.

    Healthcare Logic: * ADT^A01: Triggers a new patient entry.

        ADT^A02: Updates patient location (Transfer).

        ADT^A03: Triggers a logical/physical delete from the active census (Discharge).

📂 Project Structure
Plaintext

├── notebooks/
│   ├── 01_Bronze_Ingestion.py   # Autoloader logic
│   ├── 02_Silver_Parsing.py     # Regex and Split-based HL7 parsing
│   └── 03_Gold_State_Merge.py   # Logic for Admit/Transfer/Discharge
├── data/
│   └── sample_adt.hl7           # Sample HL7 v2 messages for testing
└── README.md

📊 Business Impact

By shifting from batch-based claims processing to real-time clinical event streaming, healthcare organizations can:

    Reduce ER wait times through live bed management.

    Automate care coordinator alerts upon patient discharge.

    Improve data accuracy for downstream Value-Based Care analytics.

How to Run

    Import the notebooks into your Databricks Workspace.

    Update the raw_data_path in the Bronze notebook to point to your storage.

    Use the provided sample_adt.hl7 file to trigger the stream.
