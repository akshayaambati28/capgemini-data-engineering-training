from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, col, avg

# Initialize Spark
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Load data
customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
orders = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

# Cast numeric column
orders = orders.withColumn("total_amount", col("total_amount").cast("double"))

# Left join (to include customers with no orders)
df = customers.join(orders, on="customer_id", how="left")

# 1. Total order amount for each customer
df_total = df.groupBy("customer_id", "first_name", "last_name").agg(
    sum("total_amount").alias("total_spent")
)

print("1. Total order amount for each customer")
df_total.show()

# 2. Top 3 customers by total spend
print("2. Top 3 customers by total spend")
df_total.orderBy(col("total_spent").desc()).limit(3).show()

# 3. Customers with no orders
df_no_orders = df.groupBy("customer_id", "first_name", "last_name").agg(
    count("sale_id").alias("order_count")
).filter(col("order_count") == 0)

print("3. Customers with no orders")
df_no_orders.show()

# 4. City-wise total revenue
df_city = df.groupBy("city").agg(
    sum("total_amount").alias("city_revenue")
)

print("4. City-wise total revenue")
df_city.show()

# 5. Average order amount per customer
df_avg = df.groupBy("customer_id", "first_name").agg(
    avg("total_amount").alias("avg_order_value")
)

print("5. Average order amount per customer")
df_avg.show()

# 6. Customers with more than one order
df_multi = df.groupBy("customer_id", "first_name").agg(
    count("sale_id").alias("order_count")
).filter(col("order_count") > 1)

print("6. Customers with more than one order")
df_multi.show()

# 7. Sort customers by total spend descending
print("7. Customers sorted by total spend (desc)")
df_total.orderBy(col("total_spent").desc()).show()
