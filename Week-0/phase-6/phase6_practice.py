# =========================================
# 🚀 PHASE 6: SPARK PLAYGROUND EXIT SPRINT
# =========================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Phase6 Practice").getOrCreate()

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad"),
    (2, "Sita", "Chennai"),
    (3, "Arun", "Hyderabad"),
    (4, "Meena", "Bangalore")
], ["customer_id", "customer_name", "city"])

orders = spark.createDataFrame([
    (101, 1, "2024-01-01", 2000),
    (102, 2, "2024-01-02", 3000),
    (103, 1, "2024-01-03", 1500),
    (104, 5, "2024-01-04", 4000)  
], ["order_id", "customer_id", "date", "amount"])

orders = orders.withColumn("date", F.to_date("date"))

# =========================================
# 🔹 PRACTICE SET A: JOINS
# =========================================

print("🔹 INNER JOIN")
inner_df = orders.join(customers, "customer_id", "inner")
inner_df.show()

print("🔹 LEFT JOIN")
left_df = orders.join(customers, "customer_id", "left")
left_df.show()

print("🔹 LEFT ANTI JOIN (invalid keys)")
anti_df = orders.join(customers, "customer_id", "left_anti")
anti_df.show()

print("🔹 ROW COUNT COMPARISON")
print("Inner:", inner_df.count())
print("Left:", left_df.count())
print("Anti:", anti_df.count())

# =========================================
# 🔹 PRACTICE SET B: WINDOW FUNCTIONS
# =========================================

# Total spend
customer_spend = inner_df.groupBy("customer_id", "city") \
    .agg(F.sum("amount").alias("total_spend"))

# Top 3 customers per city
window_city = Window.partitionBy("city").orderBy(F.col("total_spend").desc())

top_customers = customer_spend.withColumn("rank", F.row_number().over(window_city)) \
    .filter(F.col("rank") <= 3)

print("🔹 TOP CUSTOMERS PER CITY")
top_customers.show()

# Running total of sales
window_run = Window.orderBy("date")

running_total = inner_df.withColumn(
    "running_total",
    F.sum("amount").over(window_run)
)

print("🔹 RUNNING TOTAL")
running_total.show()

# Rank customers by spend
ranked_customers = customer_spend.withColumn(
    "rank",
    F.dense_rank().over(Window.orderBy(F.col("total_spend").desc()))
)

print("🔹 CUSTOMER RANKING")
ranked_customers.show()

# LAG function
lag_df = inner_df.withColumn(
    "previous_amount",
    F.lag("amount").over(window_run)
)

print("🔹 LAG FUNCTION")
lag_df.show()

# =========================================
# 🔹 PRACTICE SET C: DATE ANALYSIS
# =========================================

# Extract month
df_month = inner_df.withColumn("month", F.month("date"))

print("🔹 MONTH EXTRACTION")
df_month.show()

# Monthly sales
monthly_sales = df_month.groupBy("month") \
    .agg(F.sum("amount").alias("total_sales"))

print("🔹 MONTHLY SALES")
monthly_sales.show()

# Date difference
df_dates = inner_df.withColumn("next_date", F.lag("date").over(window_run))

df_dates = df_dates.withColumn(
    "days_diff",
    F.datediff(F.col("date"), F.col("next_date"))
)

print("🔹 DATE DIFFERENCE")
df_dates.show()

# =========================================
# 🔹 PRACTICE SET D: COMPLETE PIPELINE
# =========================================

# Clean data
clean_df = orders.dropna(subset=["customer_id", "amount"])

# Validate (remove invalid foreign keys)
valid_df = clean_df.join(customers, "customer_id", "inner")

# Join tables
final_join = valid_df

# Aggregation
final_agg = final_join.groupBy("customer_id", "city") \
    .agg(F.sum("amount").alias("total_spend"))

# Save output (local/demo)
# final_agg.write.mode("overwrite").csv("/tmp/output")

print("🔹 FINAL PIPELINE OUTPUT")
final_agg.show()


spark.stop()
