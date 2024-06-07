from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("Batch_Consumer") \
    .getOrCreate()

# Define the schema for the incoming data (without 'author')
schema = StructType() \
    .add("author", IntegerType())  # Assuming 'author' is the field you receive from Kafka

# Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "author") \
    .load()

# Convert value column to string and parse JSON with defined schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Group by the 'author' field and aggregate
windowedCounts = df \
    .groupBy("author") \
    .agg(count("*").alias("value"))

# Sort the data in ascending order
sortedCounts = windowedCounts \
    .sort(col("author").asc())

# Write the output to the console
query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

