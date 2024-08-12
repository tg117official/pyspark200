# Implement a UDF that computes the sum of lengths of all strings in a group.
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

# Create UDF
sum_length_udf = udf(lambda words: sum(len(word) for word in words), IntegerType())

# Create DataFrame
df = spark.createDataFrame([(["hello", "world"],), (["spark", "python"],)], ["words"])

# Group and apply UDF
df.groupBy().agg(sum_length_udf(col("words")).alias("total_length")).show()










from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("UDF").getOrCreate()
@udf(IntegerType())
def sum_length(words):
    return sum(len(word) for word in words)

df.groupBy().agg(sum_length(col("words")).alias("total_length")).show()






