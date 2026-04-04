# 🚀 Phase 4 – Mini Project: Business Pipeline & Analytics

## 📌 Overview

This project implements an end-to-end data pipeline using PySpark to generate business insights from customer and sales data.
It demonstrates the transition from isolated data transformations to a structured analytics workflow.

---

## 🎯 Objective

* Build a complete data pipeline using PySpark
* Perform data cleaning and validation
* Apply transformations and aggregations
* Generate actionable business insights
* Create a final reporting dataset

---

## 🔄 ETL Pipeline Workflow

### 🔹 Extract

* Loaded datasets from CSV files using PySpark
* Input sources:

  * `customers.csv`
  * `sales.csv`

### 🔹 Transform

* Cleaned data by removing null keys and invalid values
* Eliminated duplicate records
* Cast columns to appropriate data types
* Joined datasets using `customer_id`
* Applied business logic and aggregations

### 🔹 Load

* Generated analytical outputs
* Saved final dataset as CSV for reporting purposes

---

## 🧹 Data Cleaning Strategy

* Removed rows with null `customer_id`
* Filtered invalid values (e.g., negative amounts)
* Removed duplicate records
* Ensured consistent data types before processing

---

## 📊 Business Tasks Implemented

### 1️⃣ Daily Sales

* Output: `date`, `total_sales`

### 2️⃣ City-wise Revenue

* Output: `city`, `total_revenue`

### 3️⃣ Top 5 Customers

* Output: `customer_name`, `total_spend`

### 4️⃣ Repeat Customers (>1 order)

* Output: `customer_id`, `order_count`

### 5️⃣ Customer Segmentation

* Gold → total_spend > 10000
* Silver → total_spend between 5000 and 10000
* Bronze → total_spend < 5000
* Output: `customer_name`, `total_spend`, `segment`

### 6️⃣ Final Reporting Table

* Output:

  * customer_name
  * city
  * total_spend
  * order_count
  * segment

---

## 🛠️ Technologies & Tools

* **PySpark (DataFrame API & Spark SQL)**
* **Apache Spark**
* **CSV Data Format**

---

## 🧠 Key Learnings

* Data cleaning is a critical first step before any transformation
* Joins must be carefully designed to avoid incorrect results
* Aggregations help convert raw data into meaningful insights
* Structuring workflows improves scalability and readability
* PySpark enables efficient processing of large datasets

---

## 💭 Reflection

* Cleaning before joins ensures data consistency and prevents incorrect matches
* Null keys can lead to data loss or inaccurate joins
* Join order impacts performance and correctness
* SQL logic and PySpark transformations follow similar patterns
* Large-scale data introduces challenges like performance and memory management

---

## 🎯 Outcome

* Built a complete business analytics pipeline using PySpark
* Generated meaningful insights from raw datasets
* Strengthened understanding of ETL and data engineering workflows
* Developed a project suitable for portfolio and interviews

---
