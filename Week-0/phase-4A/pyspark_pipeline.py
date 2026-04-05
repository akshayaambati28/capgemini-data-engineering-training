# 🚀 PHASE 4A: BUCKETING & SEGMENTATION

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

# 1. INITIALIZE SPARK
spark = SparkSession.builder.appName("Bucketing & Segmentation").getOrCreate()

# 2. SAMPLE DATA (Customer Spend)
data = [
    (1, "Ravi", 12000),
    (2, "Sita", 7000),
    (3, "Arun", 3000),
    (4, "Meena", 15000),
    (5, "John", 4500),
    (6, "Anu", 8000),
    (7, "Kiran", 2000)
]

columns = ["customer_id", "customer_name", "total_spend"]

df = spark.createDataFrame(data, columns)

print(" Raw Data:")
df.show()

# 3. METHOD 1: CONDITIONAL LOGIC


df_conditional = df.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when((F.col("total_spend") >= 5000) & (F.col("total_spend") <= 10000), "Silver")
     .otherwise("Bronze")
)

print("📊 Conditional Segmentation:")
df_conditional.show()

# Group by segment
segment_count = df_conditional.groupBy("segment") \
    .agg(F.count("*").alias("customer_count"))

print("📊 Customers per Segment:")
segment_count.show()


# 4. METHOD 2: BUCKETIZER (MLlib)

splits = [-float("inf"), 5000, 10000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df_bucket = bucketizer.transform(df)

print("📊 Bucketizer Output:")
df_bucket.show()

# 5. METHOD 3: QUANTILE-BASED SEGMENTATION

quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1, q2 = quantiles[0], quantiles[1]

df_quantile = df.withColumn(
    "segment",
    F.when(F.col("total_spend") <= q1, "Bronze")
     .when((F.col("total_spend") > q1) & (F.col("total_spend") <= q2), "Silver")
     .otherwise("Gold")
)

print("📊 Quantile Segmentation:")
df_quantile.show()

# 6. METHOD 4: WINDOW-BASED SEGMENTATION

window_spec = Window.orderBy("total_spend")

df_window = df.withColumn(
    "rank_pct",
    F.percent_rank().over(window_spec)
)

df_window = df_window.withColumn(
    "segment",
    F.when(F.col("rank_pct") <= 0.33, "Bronze")
     .when(F.col("rank_pct") <= 0.66, "Silver")
     .otherwise("Gold")
)

print("📊 Window-Based Segmentation:")
df_window.show()

# 8. STOP
spark.stop()
