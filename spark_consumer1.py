from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter data to only include scores greater than 5
filtered_df = df.filter(col("score") > 5)

# Group by score and count
grouped_df = filtered_df.groupBy("score").count()

query = grouped_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for termination
query.awaitTermination()
