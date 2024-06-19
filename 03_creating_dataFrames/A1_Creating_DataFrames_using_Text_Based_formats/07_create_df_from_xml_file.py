# Importing necessary libraries
from pyspark.sql import SparkSession

# Creating a SparkSession with the spark-xml package
spark = SparkSession.builder \
    .appName("Read XML Data") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0") \
    .getOrCreate()

# Reading data from an XML file into a DataFrame
# Ensure the spark-xml package is included in your environment for this to work
df = spark.read.format("xml").option("rowTag", "person").\
    load(r"C:\Users\Sandeep\PycharmProjects\pyspark200\03_creating_dataFrames\data\A1_resources\persons.xml")

# Printing the schema of the DataFrame to understand the structure of the XML data
df.printSchema()

# Displaying the DataFrame to show the content
df.show()

# Stopping the SparkSession
spark.stop()
