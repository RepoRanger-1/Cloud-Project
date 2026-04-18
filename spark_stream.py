import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

DATA = os.environ.get("PIPELINE_DATA_DIR", "data")
os.makedirs(os.path.join(DATA, "output"), mode=0o755, exist_ok=True)
os.makedirs(os.path.join(DATA, "analytics"), mode=0o755, exist_ok=True)
os.makedirs(os.path.join(DATA, "checkpoints", "raw"), mode=0o755, exist_ok=True)
os.makedirs(os.path.join(DATA, "checkpoints", "mongo"), mode=0o755, exist_ok=True)
os.makedirs(os.path.join(DATA, "checkpoints", "analytics"), mode=0o755, exist_ok=True)

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

kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
mongo_uri = os.environ.get("MONGO_SPARK_URI", "mongodb://mongodb:27017")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING)")

json_df = value_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

json_df = json_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

product_df = spark.read.json("products.json")

json_df = json_df.join(product_df, on="product_id", how="left")

agg_df = json_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("event_type"),
        col("category")
    ).count()

raw_path = os.path.join(DATA, "output")
mongo_checkpoint = os.path.join(DATA, "checkpoints", "mongo")
raw_checkpoint = os.path.join(DATA, "checkpoints", "raw")
analytics_path = os.path.join(DATA, "analytics")
analytics_checkpoint = os.path.join(DATA, "checkpoints", "analytics")

raw_query = json_df.writeStream \
    .format("parquet") \
    .option("path", raw_path) \
    .option("checkpointLocation", raw_checkpoint) \
    .outputMode("append") \
    .start()

# Separate collection from Node consumer (events) to avoid duplicate _id / confusion
mongo_query = json_df.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", mongo_uri) \
    .option("spark.mongodb.database", "ecommerce") \
    .option("spark.mongodb.collection", "events_spark") \
    .option("checkpointLocation", mongo_checkpoint) \
    .outputMode("append") \
    .start()

agg_query = agg_df.writeStream \
    .format("parquet") \
    .option("path", analytics_path) \
    .option("checkpointLocation", analytics_checkpoint) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
