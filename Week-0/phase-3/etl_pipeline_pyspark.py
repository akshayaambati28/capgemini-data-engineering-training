🚀 ETL PIPELINE: CUSTOMERS + SALES

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. EXTRACT
spark = SparkSession.builder.appName("Customers-Sales ETL").getOrCreate()

customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

# 2. INSPECT
print("Customers Data:")
customers.show(5)
customers.printSchema()

print("Sales Data:")
sales.show(5)
sales.printSchema()

# 3. TRANSFORM (DATA CLEANING)

# Cast columns to correct types
sales = sales.withColumn("customer_id", F.col("customer_id").cast("int")) \
             .withColumn("quantity", F.col("quantity").cast("int")) \
             .withColumn("total_amount", F.col("total_amount").cast("double"))

customers = customers.withColumn("customer_id", F.col("customer_id").cast("int"))

# Remove nulls and invalid values
sales_clean = sales.dropna(subset=["customer_id", "quantity", "total_amount"]) \
    .filter((F.col("quantity") > 0) & (F.col("total_amount") > 0))

customers_clean = customers.dropna(subset=["customer_id"])

# 4. JOIN
df = sales_clean.join(customers_clean, "customer_id")

# 5. BUSINESS TRANSFORMATIONS

# 5.1 Daily Sales
daily_sales = sales_clean.groupBy("sale_date") \
    .agg(F.sum("total_amount").alias("daily_sales"))

# 5.2 City-wise Revenue
city_revenue = df.groupBy("city") \
    .agg(F.sum("total_amount").alias("revenue"))

# 5.3 Repeat Customers (>2 orders)
repeat_customers = sales_clean.groupBy("customer_id") \
    .agg(F.count("*").alias("order_count")) \
    .filter(F.col("order_count") > 2)

# 5.4 Highest Spending Customer per City
customer_spend = df.groupBy("city", "customer_id") \
    .agg(F.sum("total_amount").alias("total_spent"))

window_spec = Window.partitionBy("city").orderBy(F.col("total_spent").desc())

top_customers = customer_spend.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1)

# 5.5 Final Reporting Table
final_report = df.groupBy("customer_id", "city") \
    .agg(
        F.sum("total_amount").alias("total_spend"),
        F.count("*").alias("order_count")
    )

# 6. OUTPUT

print("📊 Daily Sales:")
daily_sales.show()

print("📊 City-wise Revenue:")
city_revenue.show()

print("📊 Repeat Customers:")
repeat_customers.show()

print("📊 Top Customers per City:")
top_customers.show()

print("📊 Final Report:")
final_report.show()

# 7. LOAD (Save Output)
final_report.write.mode("overwrite").parquet("/tmp/final_report")

# 8. STOP
spark.stop()
