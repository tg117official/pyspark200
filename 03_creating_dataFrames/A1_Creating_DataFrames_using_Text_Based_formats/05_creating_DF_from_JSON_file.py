# Importing necessary libraries for Spark
from pyspark.sql import SparkSession

# Creating a Spark session
spark = SparkSession.builder \
    .appName("Movie Data Analysis") \
    .getOrCreate()

# Read the movie data from a JSON file into a DataFrame
# Replace '<path to movies.json>' with the actual path to your JSON file
moviesDF = spark.read.option("inferSchema", "true").json("data/movies.json")

# Print the schema of the DataFrame to show the inferred data types and structure
moviesDF.printSchema()

# Display the contents of the DataFrame to provide insight into the movie data
moviesDF.show()

# Stop the Spark session
spark.stop()
