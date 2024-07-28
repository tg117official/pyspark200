from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('UDF Example').getOrCreate()

# Create a UDF that doubles the value
def double_num(x):
    return x * 2

# Register UDF
double_udf = udf(double_num, IntegerType())

# Create DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# Apply UDF
df.withColumn('doubled', double_udf('number')).show()





from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(IntegerType())
def double_num(x):
    return x * 2

df.withColumn('doubled', double_num('number')).show()