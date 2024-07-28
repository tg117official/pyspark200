from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinOperationsComplete").getOrCreate()

# Load the data into DataFrames
df_breakfast_orders = spark.read.option("header", "true").csv("data/breakfast_orders.txt")
df_token_details = spark.read.option("header", "true").csv("data/token_details.txt")

df_breakfast_orders.show()
df_token_details.show()