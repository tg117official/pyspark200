from pyspark.sql import SparkSession

# Initialize a SparkSession
# The SparkSession is the entry point to programming Spark with the Dataset and DataFrame API.
spark = SparkSession.builder \
    .appName("ReadORCFile") \
    .getOrCreate()

# Read the ORC file into a DataFrame
# The read.orc method is used to load an ORC file as a DataFrame. This is particularly useful for handling
# large datasets stored in an efficient binary format that is both fast to read and compact in size.
df = spark.read.orc(r"file:///C:\Users\Sandeep\PycharmProjects\pyspark500\Creating DataFrames\data\A2_resources\cr.orc")

# Show the DataFrame
# This is used to display the first few rows of the DataFrame to ensure it's loaded correctly and to inspect the data.
df.show()

# Stop the Spark session
# It's a good practice to stop the Spark session when your processing is complete to free up resources.
spark.stop()
