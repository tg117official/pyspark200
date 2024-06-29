# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Mixed Delimiters") \
    .getOrCreate()

# Reading the data from a CSV file where the primary delimiter is a comma
from pyspark.sql.functions import *

df = spark.read.option("header","true").\
csv(r"file:///C:\Users\Sandeep\Desktop\data\mixed_demiliters.txt")

df.printSchema()

df.show()

df1 = df.withColumn("id", split(col("id#name$age|salary")[0], '#'))

df2 = df1.withColumn("name", split(col("id#name$age|salary"), '\$')[0]).\
withColumn("name", split(col("name"),'#')[1])

df3 = df2.withColumn("age", split(col("id#name$age|salary"), "\$")[1]).\
withColumn("age", split(col("age"), '\|')[0])


df4 = df3.withColumn("salary", split(col("id#name$age|salary"), '\|')[1])


df5 = df4.drop("id#name$age|salary")


df5.show()


# Stopping the SparkSession
spark.stop()
