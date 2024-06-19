from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.functions import col

# Initialize a SparkSession
# Include the Avro package if necessary. This line can be modified depending on your Spark version and setup.
# For example, in Spark 3.x and above, you might not need to add the package explicitly if already included.
# Following package is for spark 3.5.1, if you have a different version, change it accourding to you version
spark = SparkSession.builder \
    .appName("ReadAvroFile") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1") \
    .getOrCreate()

# Load the Avro schema file
# It's good practice to load the schema from a file to ensure compatibility and avoid manual coding errors.
schema_path = r"C:\Users\Sandeep\PycharmProjects\pyspark500\Creating DataFrames\data\A2_resources\twitter.avsc"
with open(schema_path, 'r') as f:
    avro_schema = f.read()

# Read the Avro file into a DataFrame using the schema
# This ensures that the DataFrame conforms to the expected structure and types as defined in the Avro schema.
df = spark.read.format("avro") \
    .option("avroSchema", avro_schema) \
    .load(r"C:\Users\Sandeep\PycharmProjects\pyspark500\Creating DataFrames\data\A2_resources\twitter.avro")

# Show the DataFrame to verify the contents
df.show()

# Stop the Spark session
spark.stop()
