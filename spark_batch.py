import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, to_timestamp

DATA = os.environ.get("PIPELINE_DATA_DIR", "data")
input_path = os.path.join(DATA, "output")
batch_out = os.path.join(DATA, "batch_output")

if not os.path.isdir(input_path) or not os.listdir(input_path):
    print(f"No Parquet data yet at {input_path}. Let the streaming job run for a minute, then retry.")
    sys.exit(1)

spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(input_path)

df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

event_counts = df.groupBy("event_type").count()

revenue = df.filter(col("event_type") == "purchase") \
            .groupBy("category") \
            .sum("price") \
            .withColumnRenamed("sum(price)", "total_revenue")

time_analysis = df.groupBy(
    window(col("timestamp"), "10 minutes")
).count()

print("Category Count:")
df.groupBy("category").count().show()

print("Event Counts:")
event_counts.show()

print("Revenue by Category:")
revenue.show()

print("Time Window Analysis:")
time_analysis.show()

event_counts.write.mode("overwrite").csv(
    os.path.join(batch_out, "event_counts"), header=True
)
revenue.write.mode("overwrite").csv(
    os.path.join(batch_out, "revenue"), header=True
)
time_analysis.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("count")
).write.mode("overwrite").csv(
    os.path.join(batch_out, "time_analysis"), header=True
)
