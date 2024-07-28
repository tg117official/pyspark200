# Develop a UDF that returns a dictionary containing the original and doubled value of a number.

from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, IntegerType, StringType

# Create UDF
def double_info(x):
    return {'original': x, 'double': x * 2}

double_info_udf = udf(double_info, MapType(StringType(), IntegerType()))

# Create DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# Apply UDF
df.withColumn('info', double_info_udf('number')).show()






from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, IntegerType, StringType

@udf(MapType(StringType(), IntegerType()))
def double_info(x):
    return {'original': x, 'double': x * 2}

df.withColumn('info', double_info('number')).show()

