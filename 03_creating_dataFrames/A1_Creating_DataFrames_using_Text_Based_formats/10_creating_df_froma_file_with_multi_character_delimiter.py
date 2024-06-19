# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("Read Multi-char Delimited CSV") \
    .getOrCreate()

# Defining the schema for the CSV data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Loading the data from a CSV file using the defined schema and a custom multicharacter delimiter
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "|~|") \
    .schema(schema) \
    .load("path/to/your/multichar_delimited_data.txt")  # Adjust the path to your actual data file's location

# Showing the schema of the DataFrame to confirm structure and data types
df.printSchema()  # Corrected to call the method with ()

# Displaying the contents of the DataFrame to visualize the data
df.show()

# Stopping the SparkSession
spark.stop()
