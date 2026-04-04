from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, avg

# Initialize Spark
spark = SparkSession.builder.appName('SQL to PySpark - Phase 2').getOrCreate()

# Load Data
customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
orders = spark.read.format('csv').option('header', 'true').load('/samples/orders.csv')

# Data Cleaning
customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

# Cast numeric column
orders = orders.withColumn("order_amount", col("order_amount").cast("double"))

# Join datasets
df = customers.join(orders, on="customer_id", how="left")

# Total order amount for each customer
df_total = df.groupBy("customer_id", "customer_name", "city").agg(
    sum("order_amount").alias("total_spend")
)

print("1. Total order amount for each customer")
df_total.show()

# Top 3 customers by total spend
print("2. Top 3 customers by total spend")
df_total.orderBy(col("total_spend").desc()).limit(3).show()

# Customers with no orders
df_no_orders = df.groupBy("customer_id", "customer_name").agg(
    count("order_amount").alias("order_count")
).filter(col("order_count") == 0)

print("3. Customers with no orders")
df_no_orders.show()

# City-wise total revenue
df_city = df.groupBy("city").agg(
    sum("order_amount").alias("total_revenue")
)

print("4. City-wise total revenue")
df_city.show()

# Average order amount per customer
df_avg = df.groupBy("customer_id", "customer_name").agg(
    avg("order_amount").alias("avg_order_value")
)

print("5. Average order amount per customer")
df_avg.show()

# Customers with more than one order
df_multi = df.groupBy("customer_id", "customer_name").agg(
    count("order_amount").alias("order_count")
).filter(col("order_count") > 1)

print("6. Customers with more than one order")
df_multi.show()

# Sort customers by total spend descending
print("7. Customers sorted by total spend (desc)")
df_total.orderBy(col("total_spend").desc()).show()
