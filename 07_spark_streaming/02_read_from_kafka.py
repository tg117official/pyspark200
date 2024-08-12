from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ec2-3-145-208-232.us-east-2.compute.amazonaws.com:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convert the value column from binary to string
value_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

# Parse the JSON string into a DataFrame with the defined schema
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Start the streaming query and print data to console using continuous mode
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="0 seconds") \
    .option("checkpointLocation", "spark_data") \
    .start()

query.awaitTermination()
