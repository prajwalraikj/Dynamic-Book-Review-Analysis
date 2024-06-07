from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType

# Define the chosen topic and schema
#choice = int(input("Enter your topic number:")) # Example choice
choice=1
if choice == 1:
    topic = "score"
    schema = StructType([StructField("score", IntegerType())])
elif choice == 2:
    topic = "author"
    schema = StructType([StructField("author", StringType())])
elif choice == 3:
    topic = "awards"
    schema = StructType([StructField("age", DecimalType())])

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
#windowedCounts = df \
    #.groupBy(col("value")) \
    #.agg(count("*").alias("count"))
    
windowedCounts = df \
    .groupBy(expr("decode(value,'UTF-8')").alias("decode_value")) \
    .agg(count("*").alias("count"))

# Sort the data by the topic
sortedCounts = windowedCounts \
    .sort(col("count").desc())

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
