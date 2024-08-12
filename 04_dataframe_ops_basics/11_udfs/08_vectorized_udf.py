# Create a Pandas UDF that squares each number in a column.
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.appName("UDF").getOrCreate()
# Create Pandas UDF
@pandas_udf("integer", PandasUDFType.SCALAR)
def square(x):
    return x * x

# Create DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# Apply UDF
df.withColumn('squared', square('number')).show()






