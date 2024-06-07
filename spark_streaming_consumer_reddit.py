from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType

# Define the chosen topic and schema
choice = 2  # Example choice
if choice == 1:
    topic = "score"
    schema = StructType([StructField("score", IntegerType())])
elif choice == 2:
    topic = "comments"
    schema = StructType([StructField("comments", IntegerType())])
elif choice == 3:
    topic = "age"
    schema = StructType([StructField("age", StringType())])

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .master("local[*]") \
    .getOrCreate()

# Create the streaming dataframe
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load() \

# Define the window and group by clauses
windowedCounts = df \
    .groupBy(col(topic)) \
    .agg(count("*").alias("count"))

# Sort the data by the topic
sortedCounts = windowedCounts \
    .sort(col(topic).asc())

# Write the output to the console
console_query = sortedCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
console_query.awaitTermination()

# Stop the query
console_query.stop()

