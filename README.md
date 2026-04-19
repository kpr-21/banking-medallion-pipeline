# 🏦 Banking Medallion Data Pipeline

## 🚀 Project Overview

This project implements an end-to-end **Data Engineering Pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)**.

It simulates a real-world **banking system** processing:
- Customers
- Accounts
- Transactions

The pipeline is orchestrated using **Apache Airflow**, processed with **PySpark**, and stored using **Delta Lake**.

---

## 🏗️ Architecture Diagram
            ┌────────────────────┐
            │   Landing Layer    │
            │   Raw CSV Files    │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │  Ingestion Layer   │
            │ Convert → Parquet  │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │   Bronze Layer     │
            │  Raw Delta Tables  │
            │ (Append Mode)      │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │   Silver Layer     │
            │ Clean + Transform  │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │    Gold Layer      │
            │ Business Insights  │
            └────────────────────┘


### 🔁 Data Flow
Landing → Ingestion → Bronze → Silver → Gold

### ⚙️ Orchestration
- Managed using **Apache Airflow DAG**
- Parallel execution for datasets (customers, accounts, transactions)

### 🔥 Processing
- Done using **PySpark**

### 💾 Storage
- **Delta Lake** for ACID transactions
- Parquet for intermediate storage

---

## ⚙️ Tech Stack

| Layer            | Technology |
|-----------------|-----------|
| Orchestration   | Apache Airflow |
| Processing      | PySpark |
| Storage         | Delta Lake / Parquet |
| Containerization| Docker |
| Language        | Python |

---

## 📂 Project Structure
├── dags/
│ └── banking_pipeline_dag.py
├── spark_jobs/
│ ├── ingestion/
│ ├── bronze/
│ ├── silver/
│ └── gold/
├── data/
│ ├── landing/
│ ├── bronze/
│ ├── silver/
│ └── gold/
├── logs/
├── Dockerfile-airflow
├── docker-compose.yml
└── README.md


---

## 🔄 Pipeline Flow

### 1️⃣ Ingestion Layer
- Reads raw CSV files
- Adds `process_date`
- Converts to Parquet
- Uses **overwrite mode**
- Ensures no duplication on reruns

---

### 2️⃣ Bronze Layer
- Loads all ingested data
- Stores in **Delta format**
- Uses **append mode**
- Moves processed files to avoid duplicates

---

### 3️⃣ Silver Layer
- Data cleaning:
  - Null handling
  - Type casting
  - Deduplication
- Adds derived columns

---

### 4️⃣ Gold Layer

#### 📊 Aggregations Implemented

### 🔹 Customer Transaction Summary
- Total transactions
- Total / Avg / Max transaction amount

### 🔹 Account Balance Summary
- Account distribution by type
- Balance statistics

### 🔹 Transaction Type Summary
- Spending patterns

### 🔹 Customer 360 View ⭐
- Combines:
  - Customers
  - Accounts
  - Transactions

---

## 🧠 Key Engineering Concepts

- Medallion Architecture
- Idempotent pipeline design
- Partitioning using `process_date`
- Delta Lake integration
- Airflow DAG orchestration
- Parallel vs sequential execution
- Duplicate data handling
- Data lineage tracking

---

## 🔁 Airflow DAG Flow
Ingestion
↓
Bronze (Parallel)
↓
Silver (Parallel)
↓
Gold (Aggregations)

---

## 🐳 Running the Project

### 1️⃣ Build Images
```bash
docker compose build
docker compose up -d

Access Airflow UI
http://localhost:8080

Create Admin User
docker exec -it airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

Future Improvements: 
Streaming ingestion (Kafka)
Data quality checks (Great Expectations)
Cloud deployment (AWS / Azure)
Dashboard integration (Power BI / Tableau)
