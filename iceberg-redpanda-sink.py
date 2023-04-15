import os
import findspark
import pyiceberg
from pyspark.sql import SparkSession
from pyspark.sql import functions

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0'
findspark.init()

# Create a Spark Session
spark = SparkSession \
  .builder \
  .appName("Redpanda Iceberg Sink") \
  .getOrCreate()

# Start reading data into a data frame by subscribing to the Redpanda topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rp_topic") \
    .load()

# Start reading from the data frame into the Iceberg table
df.selectExpr("CAST(value AS STRING)") \
    .writeStream.format("iceberg") \
    .outputMode("append") \
    .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS)) \
    .option("path", "redpanda.rp_topic") \
    .option("checkpointLocation", "checkpoint") \
    .start()
