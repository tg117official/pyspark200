# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Mixed Delimiters") \
    .getOrCreate()

# Reading the data from a CSV file where the primary delimiter is a comma
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .load("path/to/your/mixed_delimiters_data.txt")  # Adjust this path to your data file's location

# Splitting the 'name|age' column into separate 'name' and 'age' columns using the pipe '|' as a secondary delimiter
split_col = split(df['name|age'], '\|')

# Adding the split columns back to the DataFrame
df = df.withColumn('name', split_col.getItem(0)) \
       .withColumn('age', split_col.getItem(1).cast('integer')) \
       .drop('name|age')

# Displaying the schema of the DataFrame to confirm the structure and data types
df.printSchema()

# Showing the contents of the DataFrame to verify it is loaded and split correctly
df.show()

# Stopping the SparkSession
spark.stop()
