# Write a UDF that adds a specified number of days to a date column.

from datetime import timedelta
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SQL UDF Example').getOrCreate()

# Create UDF
def add_days(date, days):
    return date + timedelta(days=days)

add_days_udf = udf(add_days, DateType())

# Create DataFrame
df = spark.createDataFrame([("2022-01-01", 10), ("2022-01-15", -5)], ["date", "days"])
df = df.select(col("date").cast("date"), "days")

# Apply UDF
df.withColumn('new_date', add_days_udf('date', 'days')).show()









from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('SQL UDF Example').getOrCreate()

# Define and register UDF
@udf(IntegerType())
def double_num(x):
    return x * 2
spark.udf.register("double_num_sql", double_num)

# Create DataFrame and register as temp view
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])
df.createOrReplaceTempView('numbers')

# Use SQL to apply UDF
spark.sql("SELECT number, double_num_sql(number) AS doubled FROM numbers").show()









from datetime import timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType

@udf(DateType())
def add_days(date, days):
    return date + timedelta(days=days)

df.withColumn('new_date', add_days('date', 'days')).show()


