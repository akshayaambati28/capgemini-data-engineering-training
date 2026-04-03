CREATE TABLE customers (
customer_id INT,
customer_name VARCHAR(50),
city VARCHAR(50),
age INT
);
INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28);
SELECT * FROM customers;
SELECT *
FROM customers
WHERE city = 'Chennai';
SELECT *
FROM customers
WHERE age > 25;
SELECT customer_name, city
FROM customers;
SELECT COUNT(*) AS total_customers
FROM customers;
