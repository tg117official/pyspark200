from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("ColumnReferenceDemo").getOrCreate()

# 1st way
sc = spark.sparkContext
dataRDD = sc.textFile("/data/employee.csv")


# 2nd way
dataRDD = spark.sparkContext.textFile("/data/employee.csv")
