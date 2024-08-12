#  Create a UDF that takes two columns, multiplies them, and returns the product.
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("UDF").getOrCreate()

# Create UDF
multiply_udf = udf(lambda x, y: x * y, IntegerType())

# Create DataFrame
df = spark.createDataFrame([(1, 2), (3, 4)], ['a', 'b'])

# Apply UDF
df.withColumn('product', multiply_udf('a', 'b')).show()








from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def multiply(x, y):
    return x * y

df.withColumn('product', multiply('a', 'b')).show()
