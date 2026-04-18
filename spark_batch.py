from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, to_timestamp

spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .getOrCreate()

# Read data
df = spark.read.parquet("data/output/")

# Cleaning
df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 1. Event count
event_counts = df.groupBy("event_type").count()

# 2. Revenue by category
revenue = df.filter(col("event_type") == "purchase") \
            .groupBy("category") \
            .sum("price") \
            .withColumnRenamed("sum(price)", "total_revenue")

# 3. Time analysis
time_analysis = df.groupBy(
    window(col("timestamp"), "10 minutes")
).count()

# Show outputs
print("Category Count:")
df.groupBy("category").count().show()

print("Event Counts:")
event_counts.show()

print("Revenue by Category:")
revenue.show()

print("Time Window Analysis:")
time_analysis.show()

# Save outputs
event_counts.write.mode("overwrite").csv("data/batch_output/event_counts/")
revenue.write.mode("overwrite").csv("data/batch_output/revenue/")
time_analysis.write.mode("overwrite").csv("data/batch_output/time_analysis/")