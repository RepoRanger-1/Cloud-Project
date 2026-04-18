from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("price", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING)")

json_df = value_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# 🔹 convert timestamp
json_df = json_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# 🔹 load static product data
product_df = spark.read.json("products.json")

# 🔹 join (enrichment)
json_df = json_df.join(product_df, on="product_id", how="left")

# 🔹 window aggregation
agg_df = json_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("event_type"),
        col("category")
    ).count()

# raw_query = json_df.writeStream \
#     .format("parquet") \
#     .option("path", "data/output/") \
#     .option("checkpointLocation", "data/checkpoints/raw") \
#     .outputMode("append") \
#     .start() 

# mongo_query = json_df.writeStream \
#     .format("mongodb") \
#     .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
#     .option("spark.mongodb.database", "ecommerce") \
#     .option("spark.mongodb.collection", "events") \
#     .option("checkpointLocation", "data/checkpoints/mongo") \
#     .outputMode("append") \
#     .start()

# agg_query = agg_df.writeStream \
#     .format("parquet") \
#     .option("path", "data/analytics/") \
#     .option("checkpointLocation", "data/checkpoints/analytics") \
#     .outputMode("complete") \
#     .start()

# query = agg_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# mongo_query = json_df.writeStream \
#     .format("mongodb") \
#     .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
#     .option("spark.mongodb.database", "ecommerce") \
#     .option("spark.mongodb.collection", "events") \
#     .option("checkpointLocation", "/tmp/checkpoints/mongo") \
#     .outputMode("append") \
#     .start()

raw_query = json_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/data/output") \
    .option("checkpointLocation", "/tmp/checkpoints/raw") \
    .outputMode("append") \
    .start()

mongo_query = json_df.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
    .option("spark.mongodb.database", "ecommerce") \
    .option("spark.mongodb.collection", "events") \
    .option("checkpointLocation", "/tmp/checkpoints/mongo") \
    .outputMode("append") \
    .start()

agg_query = agg_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/data/analytics") \
    .option("checkpointLocation", "/tmp/checkpoints/analytics") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()