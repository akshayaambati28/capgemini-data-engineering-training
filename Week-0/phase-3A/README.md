# 🚀 Phase 3A – Data Quality & Cleaning Challenge

## 📌 Overview

This phase focuses on handling real-world messy data and applying data cleaning techniques before performing analysis.
The objective is to understand the importance of data quality in building reliable data pipelines.

---

## 🎯 Objective

* Identify common data quality issues
* Apply cleaning techniques using PySpark
* Validate cleaned data
* Perform aggregation after cleaning

---

## ⚠️ Data Issues Identified

* Missing values (nulls in customer_id, name, city)
* Duplicate records
* Invalid values (negative age)

---

## 🛠️ Cleaning Steps Performed

* Removed rows with null `customer_id` (primary key)
* Eliminated duplicate records
* Handled missing values using default replacements
* Filtered invalid age values (`age > 0`)

---

## 📊 Validation

* Compared row counts before and after cleaning
* Ensured only valid and consistent data is retained

---

## 📈 Analysis Performed

* Calculated number of customers per city using aggregation

---

## 🧠 Key Learnings

* Real-world datasets often contain inconsistencies
* Data cleaning is essential before any transformation or analysis
* Poor data quality can lead to incorrect insights
* Validation is a critical step in data processing

---

## 💭 Reflection

* Skipping cleaning can result in incorrect aggregations and misleading insights
* Null values and duplicates significantly impact results
* Invalid data can lead to poor business decisions
* A structured cleaning strategy is essential in every ETL pipeline

---

## 🎯 Outcome

* Developed practical skills in handling messy data
* Strengthened understanding of data validation and preprocessing
* Improved ability to prepare datasets for reliable analysis

---
