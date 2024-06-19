# Create a DatFrame from employee.txt (csv) by providing schema explicitly

# Importing necessary libraries for Spark and data types
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Creating a Spark session
spark = SparkSession.builder \
    .appName("Employee Data with Custom Schema") \
    .getOrCreate()

# Define a custom schema for the employee DataFrame
emp_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("date_of_joining", StringType(), True)
])

# Read the employee data from a CSV file into a DataFrame using the defined schema
# Replace '<path to employee.csv>' with the actual path to your CSV file
empDF = spark.read.option("header", "true").schema(emp_schema).csv("file:///<path to employee.csv>")

# Print the schema of the DataFrame to show the data types and structure
empDF.printSchema()

# Display the contents of the DataFrame to provide an overview of the data
empDF.show()

# Stop the Spark session
spark.stop()
