# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Schema") \
    .getOrCreate()

# Defining the schema for the CSV data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Loading the data from a pipe-delimited CSV file using the defined schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .schema(schema) \
    .load("data/users_pipe_separated.txt")  # Adjust the path to where your actual data file is located

# Showing the schema of the DataFrame to confirm structure and data types
df.printSchema()

# Displaying the contents of the DataFrame to visualize the data
df.show()

# Stopping the SparkSession
spark.stop()
