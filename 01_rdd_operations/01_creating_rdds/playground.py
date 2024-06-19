from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSVToSequenceFile") \
    .getOrCreate()

# Read CSV into DataFrame
df = spark.read.option("header", "true").csv("data/movies.csv")

# df.show()
# Convert DataFrame to RDD
rdd = df.rdd.map(lambda row: (row[0], ' '.join([str(x) for x in row])))

# Save RDD as Sequence File
rdd.saveAsSequenceFile("data/sequence_data")

# Stop Spark Session
spark.stop()
