# 🚀 PHASE 3A: DATA QUALITY & CLEANING

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. INITIALIZE SPARK
spark = SparkSession.builder.appName("Data Cleaning Challenge").getOrCreate()

# 2. CREATE MESSY DATA
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Raw Data:")
df.show()

# 3. IDENTIFY DATA ISSUES
print("Total Rows (Before Cleaning):", df.count())

# 4. DATA CLEANING

# Remove rows with null customer_id (primary key)
df_clean = df.dropna(subset=["customer_id"])

# Remove duplicates
df_clean = df_clean.dropDuplicates()

# Handle missing values (optional: fill)
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Filter invalid age
df_clean = df_clean.filter(F.col("age") > 0)

print("Cleaned Data:")
df_clean.show()

print("Total Rows (After Cleaning):", df_clean.count())

# 5. AGGREGATION (Customers per City)
city_count = df_clean.groupBy("city") \
    .agg(F.count("customer_id").alias("customer_count"))

print("📊 Customers per City:")
city_count.show()

# 6. STOP
spark.stop()
