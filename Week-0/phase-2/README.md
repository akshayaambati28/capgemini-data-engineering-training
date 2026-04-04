🚀 SQL to PySpark – Phase 2 (Revised Bridge Pack)

📌 Overview

This phase focuses on bridging the gap between basic SQL concepts and real-world PySpark data engineering tasks.
Using sample datasets, I performed data cleaning, joins, and aggregations to solve practical business problems.


🎯 Objective
Strengthen SQL to PySpark conversion skills
Work with real datasets instead of static examples
Perform data cleaning and handle missing values
Implement joins and aggregations for analysis


📂 Datasets Used

customers.csv → Contains customer details
orders.csv → Contains transaction/order data


🛠️ Key Operations Performed

Reading CSV data using PySpark
Inspecting schema with printSchema()
Data cleaning using dropna()
Casting columns to appropriate data types
Performing joins between datasets
Applying aggregations using:
sum()
avg()
count()
Sorting and filtering results


🔄 Problems Solved

Total order amount for each customer
Top 3 customers by total spend
Customers with no orders
City-wise total revenue
Average order amount per customer
Customers with more than one order
Sorting customers by total spend


🧹 Data Cleaning Step

Rows with missing customer_id were removed from both datasets to ensure accurate joins and aggregations.


🧠 Key Learnings

Real-world data often contains missing or inconsistent values
Data cleaning is a critical first step before analysis
PySpark joins closely resemble SQL joins but require careful column handling
Aggregations in PySpark follow a transformation-based approach


🎯 Outcome

Successfully implemented real-world queries using PySpark
Improved understanding of joins, aggregations, and data cleaning
Built confidence in translating SQL logic into PySpark workflows
