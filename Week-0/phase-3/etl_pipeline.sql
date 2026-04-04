🚀 PHASE 3: SQL ETL PIPELINE

-- 1. EXTRACT (Load Data)

CREATE OR REPLACE TEMP VIEW customers
USING csv
OPTIONS (
path "/samples/customers.csv",
header "true",
inferSchema "true"
);

CREATE OR REPLACE TEMP VIEW sales
USING csv
OPTIONS (
path "/samples/sales.csv",
header "true",
inferSchema "true"
);

-- 2. TRANSFORM – DATA CLEANING

CREATE OR REPLACE TEMP VIEW customers_clean AS
SELECT *
FROM customers
WHERE customer_id IS NOT NULL;

CREATE OR REPLACE TEMP VIEW sales_clean AS
SELECT *
FROM sales
WHERE customer_id IS NOT NULL
AND quantity IS NOT NULL
AND total_amount IS NOT NULL
AND quantity > 0
AND total_amount > 0;

-- 3. DAILY SALES

CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT
sale_date,
SUM(total_amount) AS daily_sales
FROM sales_clean
GROUP BY sale_date;

-- 4. CITY-WISE REVENUE

CREATE OR REPLACE TEMP VIEW city_revenue AS
SELECT
c.city,
SUM(s.total_amount) AS revenue
FROM sales_clean s
JOIN customers_clean c
ON s.customer_id = c.customer_id
GROUP BY c.city;

-- 5. REPEAT CUSTOMERS (>2 ORDERS)

CREATE OR REPLACE TEMP VIEW repeat_customers AS
SELECT
customer_id,
COUNT(*) AS order_count
FROM sales_clean
GROUP BY customer_id
HAVING COUNT(*) > 2;

-- 6. TOP CUSTOMER PER CITY

CREATE OR REPLACE TEMP VIEW top_customers AS
SELECT city, customer_id, total_spent
FROM (
SELECT
c.city,
s.customer_id,
SUM(s.total_amount) AS total_spent,
ROW_NUMBER() OVER (
PARTITION BY c.city
ORDER BY SUM(s.total_amount) DESC
) AS rn
FROM sales_clean s
JOIN customers_clean c
ON s.customer_id = c.customer_id
GROUP BY c.city, s.customer_id
) t
WHERE rn = 1;

-- 7. FINAL REPORT

CREATE OR REPLACE TEMP VIEW final_report AS
SELECT
c.customer_id,
c.city,
SUM(s.total_amount) AS total_spend,
COUNT(*) AS order_count
FROM sales_clean s
JOIN customers_clean c
ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.city;

-- 8. LOAD 

CREATE TABLE IF NOT EXISTS final_report_table
USING parquet
AS
SELECT * FROM final_report;

-- 9. VALIDATION

SELECT * FROM daily_sales;
SELECT * FROM city_revenue;
SELECT * FROM repeat_customers;
SELECT * FROM top_customers;
SELECT * FROM final_report;

