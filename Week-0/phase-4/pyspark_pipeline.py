# 🚀 PHASE 4: BUSINESS PIPELINE & ANALYTICS

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. EXTRACT
spark = SparkSession.builder.appName("Business Pipeline").getOrCreate()

customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

# 2. TRANSFORM (DATA CLEANING)

# Cast columns
customers = customers.withColumn("customer_id", F.col("customer_id").cast("int"))

sales = sales.withColumn("customer_id", F.col("customer_id").cast("int")) \
             .withColumn("quantity", F.col("quantity").cast("int")) \
             .withColumn("total_amount", F.col("total_amount").cast("double"))

# Remove null keys
customers_clean = customers.dropna(subset=["customer_id"])
sales_clean = sales.dropna(subset=["customer_id", "total_amount"])

# Remove duplicates
customers_clean = customers_clean.dropDuplicates()
sales_clean = sales_clean.dropDuplicates()

# Filter invalid values
sales_clean = sales_clean.filter(F.col("total_amount") > 0)

# 3. JOIN
df = sales_clean.join(customers_clean, "customer_id")

# 4. BUSINESS TASKS

# 4.1 Daily Sales
daily_sales = sales_clean.groupBy("sale_date") \
    .agg(F.sum("total_amount").alias("total_sales"))

# 4.2 City-wise Revenue
city_revenue = df.groupBy("city") \
    .agg(F.sum("total_amount").alias("total_revenue"))

# 4.3 Top 5 Customers
top_customers = df.groupBy("customer_name") \
    .agg(F.sum("total_amount").alias("total_spend")) \
    .orderBy(F.col("total_spend").desc()) \
    .limit(5)

# 4.4 Repeat Customers (>1 order)
repeat_customers = sales_clean.groupBy("customer_id") \
    .agg(F.count("*").alias("order_count")) \
    .filter(F.col("order_count") > 1)

# 4.5 Customer Segmentation
customer_spend = df.groupBy("customer_id", "customer_name", "city") \
    .agg(F.sum("total_amount").alias("total_spend"))

segmented = customer_spend.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
     .otherwise("Bronze")
)

# 4.6 Final Reporting Table
final_df = segmented.join(repeat_customers, "customer_id", "left") \
    .fillna({"order_count": 0}) \
    .select("customer_name", "city", "total_spend", "order_count", "segment")

# 5. OUTPUT

print("Daily Sales:")
daily_sales.show()

print("City-wise Revenue:")
city_revenue.show()

print("Top 5 Customers:")
top_customers.show()

print("Repeat Customers:")
repeat_customers.show()

print("Final Report:")
final_df.show()

# 6. LOAD
final_df.write.mode("overwrite").csv("/samples/output/report", header=True)

# 7. STOP
spark.stop()
