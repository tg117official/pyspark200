from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("EmployeeDataAnalysis").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 1: Select All Columns
df.show()