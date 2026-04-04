# 🚀 Phase 2 – SQL to PySpark (Data Transformation & Joins)

## 📌 Overview

This phase focuses on applying SQL-to-PySpark concepts to real-world datasets.
It introduces data cleaning, joins, and aggregations, bridging the gap between foundational concepts and practical data engineering tasks.

---

## 🎯 Objective

* Work with real datasets instead of static examples
* Perform data cleaning and validation
* Implement joins and aggregations
* Translate SQL queries into PySpark transformations

---

## 🛠️ Operations Performed

* Data ingestion using `spark.read()`
* Schema inspection using `printSchema()`
* Data cleaning using `dropna()`
* Joining datasets using `join()`
* Aggregations using:

  * `sum()`
  * `avg()`
  * `count()`
* Sorting and filtering results

---

## 📊 Tasks Implemented

* Calculated total order amount per customer
* Identified top customers by total spend
* Found customers with no orders
* Computed city-wise revenue
* Calculated average order value per customer
* Identified customers with multiple orders

---

## ⚠️ Data Challenges Identified

* Missing values in key columns
* Data type inconsistencies
* Handling joins without ambiguity

---

## 🧹 Cleaning Approach

* Removed rows with missing `customer_id`
* Ensured valid data before performing joins and aggregations
* Applied filtering to maintain data integrity

---

## 🧠 Key Learnings

* Data cleaning is essential before performing transformations
* Joins in PySpark require careful column handling
* Aggregations in PySpark follow a transformation-based approach
* SQL knowledge significantly simplifies PySpark learning

---

## 🎯 Outcome

* Successfully handled real-world datasets using PySpark
* Gained experience in joins, aggregations, and data cleaning
* Improved ability to convert SQL logic into PySpark workflows

---
