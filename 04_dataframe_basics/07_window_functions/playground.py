from pyspark.sql import SparkSession

katappa = SparkSession.builder.appName("demo").getOrCreate()
spark_2 = SparkSession.builder.appName("demo").getOrCreate()

print(katappa.sparkContext)
print(spark_2.sparkContext)

spark_2.stop()
katappa.stop()
