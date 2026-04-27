# 🛍 Retail Lakehouse Pipeline (Databricks + PySpark)

End-to-end data engineering project implementing a modern **Medallion Architecture (Bronze → Silver → Gold)** using PySpark and Delta Lake in Databricks.

---

## 🚀 Project Overview

This project simulates a real-world retail data pipeline, including messy transactional data (duplicates, nulls, bad records, and late-arriving data), and transforms it into clean, analytics-ready datasets.

---

## 🧱 Architecture

```
Raw CSV → Bronze → Silver → Gold → Analytics / Dashboard
```

### 🥉 Bronze Layer

* Ingest raw transactional data
* Add ingestion metadata
* Preserve source data for traceability

### 🥈 Silver Layer

* Deduplicate using window functions
* Handle nulls and enforce schema
* Filter invalid records
* Add data quality flags

### 🥇 Gold Layer

* Daily revenue metrics
* Product performance
* Customer lifetime value (LTV)
* Store-level analytics

---

## ⚙️ Tech Stack

* PySpark
* Delta Lake
* Databricks
* Python (data generation)
* Git + GitHub

---

## 📊 Example Metrics

* Total daily revenue trends
* Top-performing products by revenue
* Customer lifetime value rankings
* Revenue by payment method and store

---

## 🧪 Data Simulation

Custom Python data generator introduces:

* Duplicate transactions
* Null values
* Negative/invalid prices
* Late-arriving records

---

## 🧠 Key Skills Demonstrated

* Data pipeline design (Medallion architecture)
* PySpark transformations (window functions, aggregations)
* Data quality handling and validation
* Delta Lake table management
* End-to-end workflow (local → GitHub → Databricks)

---

## 📂 Project Structure

```
retail-lakehouse-pipeline/
│
├── data_generator/
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformations
│   └── 03_gold_analytics
│
├── src/
├── docs/
├── README.md
└── requirements.txt
```

---

## 🚀 How to Run

1. Generate dataset locally:

```bash
python data_generator/generate_transactions.py
```

2. Upload data to Databricks

3. Run notebooks in order:

* Bronze → Silver → Gold

---

## 📈 Future Improvements

* Incremental processing (MERGE / CDC)
* Streaming ingestion
* Data quality monitoring
* Dashboard integration

---

## 👩‍💻 Author

Brandy Patenaude
Data Engineer (ETL, PySpark, Databricks)