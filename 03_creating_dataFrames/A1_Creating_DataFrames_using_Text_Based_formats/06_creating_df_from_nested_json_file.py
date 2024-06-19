# Importing necessary libraries

from pyspark.sql import SparkSession


# Creating a SparkSession

spark = SparkSession.builder \
        .appName("Read Nested JSON") \
        .getOrCreate()


# Reading a JSON file with nested data, the option "multiLine" set to True allows reading multi-line JSON files
df = spark.read.option("multiLine", "true").json(r"file:///C:\Users\Sandeep\Desktop\data\various_formats\nested_json.json")


# Printing the schema of the DataFrame to understand the structure of the nested JSON data
df.printSchema()


# Displaying the contents of the DataFrame in a tabular format
df.show()


# Stopping the SparkSession
spark.stop()