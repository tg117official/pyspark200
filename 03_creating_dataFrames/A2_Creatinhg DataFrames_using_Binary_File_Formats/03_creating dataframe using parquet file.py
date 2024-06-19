from pyspark.sql import SparkSession

# Initialize a SparkSession
# The SparkSession is the entry point for programming with the Dataset and DataFrame API.
spark = SparkSession.builder \
    .appName("ReadParquetFile") \
    .getOrCreate()

# Read the Parquet file into a DataFrame
# Parquet is a columnar storage file format that is optimized for use with data processing frameworks like Spark.
# It supports efficient compression and encoding schemes, which saves storage space and allows faster data processing.
df = spark.read.parquet(r"file:///C:\Users\Sandeep\PycharmProjects\pyspark500\Creating DataFrames\data\A2_resources\movies.parquet")

# Show the DataFrame
# This command is useful for displaying the first few rows of the DataFrame to confirm that the data has been loaded correctly.
df.show()

# Stop the Spark session
# Closing the Spark session releases the resources allocated by Spark and is considered good practice when the processing is complete.
spark.stop()
