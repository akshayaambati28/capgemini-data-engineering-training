# 🚀 PySpark ETL Pipeline: Customers & Sales Data

## 📌 Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using PySpark to process and analyze customer and sales data.
It demonstrates how raw data can be ingested, cleaned, transformed, and structured into meaningful insights suitable for reporting and downstream analytics.

---

## 🎯 Objectives

* Design and implement a scalable ETL pipeline using PySpark
* Perform data cleaning and validation on real-world datasets
* Apply transformations such as joins, aggregations, and window functions
* Generate analytical outputs to support business decision-making

---

## 🔄 ETL Pipeline Architecture

### 🔹 Extract

* Ingested structured data from CSV files using PySpark DataFrame API
* Source datasets:

  * `customers.csv` (customer information)
  * `sales.csv` (transaction data)

### 🔹 Transform

* Enforced schema consistency through explicit type casting
* Handled missing and invalid data using filtering and null removal
* Joined datasets on `customer_id` to create a unified dataset
* Applied business transformations including:

  * Aggregations (`sum`, `count`)
  * Window functions (`row_number`)
  * Conditional filtering

### 🔹 Load

* Generated intermediate and final analytical outputs
* Persisted the final dataset in **Parquet format** for efficient storage and retrieval

---

## 🛠️ Technologies & Tools

* **PySpark (Spark SQL & DataFrame API)**
* **Apache Spark**
* **CSV & Parquet file formats**

---

## 📊 Key Analytical Outputs

* Daily sales trends
* City-wise revenue distribution
* Identification of repeat customers (>2 orders)
* Highest spending customer in each city
* Final aggregated reporting dataset containing:

  * Customer ID
  * City
  * Total Spend
  * Order Count

---

## 🧠 Key Learnings

* Developed a strong understanding of ETL pipeline design
* Gained experience in handling data quality issues and schema inconsistencies
* Improved proficiency in translating SQL logic into PySpark transformations
* Learned to structure scalable and maintainable data workflows

---

## 🎯 Outcomes

* Successfully built a modular and reusable ETL pipeline
* Demonstrated the ability to process and analyze structured data at scale
* Strengthened practical knowledge of data engineering concepts

---
