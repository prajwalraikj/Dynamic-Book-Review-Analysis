from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Batch_Consumer") \
    .getOrCreate()

schema = StructType() \
    .add("score", IntegerType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "score") \
    .load()

# Cast the value column to string
df = df.withColumn("value_string", col("value").cast("string"))

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr("data.score", "current_timestamp() as timestamp")

window_duration = "5 minutes"
windowed_df = df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), window_duration), "score") \
    .count()

query = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for termination
query.awaitTermination()