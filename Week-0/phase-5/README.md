# 🚀 Phase 5 – End-to-End Data Engineering Pipeline (Databricks + Olist Dataset)

## 📌 Overview

This project implements a complete end-to-end data engineering pipeline using the Olist Brazilian E-commerce dataset in Databricks.
It covers data ingestion, validation, modeling, advanced analytics using window functions, and building a final reporting dataset.

---

## 🎯 Objective

* Work with a real-world multi-table dataset
* Perform data ingestion and validation in Databricks
* Apply joins, aggregations, and window functions
* Build a scalable data pipeline for business analytics
* Generate a final reporting dataset

---

## 📂 Dataset

* **Olist Brazilian E-commerce Dataset (Kaggle)**
* Multiple related tables (customers, orders, products, payments, etc.)
* Uploaded and processed in Databricks environment 

---

## 🔄 ETL Pipeline Workflow

### 🔹 Extract

* Uploaded and read multiple CSV files from `/FileStore/olist/`
* Loaded datasets into PySpark DataFrames

---

### 🔹 Transform

* Performed data cleaning and validation
* Ensured referential integrity between tables
* Applied joins across multiple datasets
* Used aggregations for business metrics
* Implemented window functions for ranking and cumulative analysis

---

### 🔹 Load

* Generated analytical outputs
* Built final reporting dataset
* Stored results for further analysis

---

## 🛠️ Key Analytical Tasks

### 1️⃣ Top 3 Customers per City

* Calculated total spend per customer
* Used window functions (`ROW_NUMBER`) to rank customers within each city

---

### 2️⃣ Running Total of Sales

* Computed daily sales
* Applied cumulative sum using window functions

---

### 3️⃣ Top Products per Category

* Aggregated product sales
* Joined with category data
* Ranked using `DENSE_RANK()`

---

### 4️⃣ Customer Lifetime Value (CLV)

* Calculated total spend per customer across all orders

---

### 5️⃣ Customer Segmentation

* Applied business rules:

  * Gold → total_spend > 10000
  * Silver → 5000–10000
  * Bronze → <5000
* Grouped customers by segment

---

### 6️⃣ Final Reporting Table

* Combined insights into a single dataset:

  * customer_id
  * city
  * total_spend
  * segment
  * total_orders

---

## 🧠 Key Learnings

* Handling complex multi-table datasets
* Understanding fact and dimension tables
* Validating joins and maintaining data integrity
* Using window functions for advanced analytics
* Designing scalable ETL pipelines in Databricks

---

## 💭 Reflection

* Data validation is critical when working with multiple tables
* Window functions simplify complex analytical queries
* Join strategies directly impact accuracy and performance
* Real-world datasets introduce challenges in schema consistency and relationships

---

## 🎯 Outcome

* Built a complete data engineering pipeline on a real-world dataset
* Gained hands-on experience with Databricks and PySpark
* Applied advanced transformations and analytics
* Developed strong problem-solving and pipeline design skills

---
