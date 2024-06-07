from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DecimalType, StructField

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("Batch_Consumer") \
    .getOrCreate()

# Define the schema for the incoming data (without 'score')
schema = StructType([StructField("awards", DecimalType())])  # Assuming 'score' is the field you receive from Kafka

# Read from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "score") \
    .load()

# Group by the 'score' field and aggregate
windowedCounts = df \
    .groupBy(expr("decode(value,'UTF-8')").alias("decode_value")) \
    .agg(count("*").alias("count"))

# Sort the data in ascending order
sortedCounts = windowedCounts \
    .sort(col("count").asc())

# Write the output to the console
query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
