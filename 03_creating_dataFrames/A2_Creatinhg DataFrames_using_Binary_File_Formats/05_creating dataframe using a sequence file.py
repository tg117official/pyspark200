from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

# Initialize Spark Context
sc = SparkContext(appName="ReadSequenceFile")

# Initialize Spark Session (needed for DataFrame creation)
spark = SparkSession(sc)

# Read the sequence file into an RDD of key-value pairs
rdd = sc.sequenceFile(r"file:///C:\Users\Sandeep\PycharmProjects\pyspark500\Creating DataFrames\data\A2_resources\employee")

# Convert RDD of tuples into RDD of Row objects
# Assuming the keys are employee IDs (strings) and the values are names (strings)
row_rdd = rdd.map(lambda kv: Row(employee_id=kv[0], employee_name=kv[1]))

# Create DataFrame from RDD of Rows
df = spark.createDataFrame(row_rdd)

# Show the DataFrame to verify it loaded correctly
df.show()

# Stop the SparkContext
sc.stop()
