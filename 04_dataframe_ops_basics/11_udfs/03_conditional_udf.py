# Create a UDF that returns 'even' if a number is even and 'odd' if a number is odd.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create UDF
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

odd_even_udf = udf(odd_even, StringType())

# Create DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# Apply UDF
df.withColumn('odd_or_even', odd_even_udf('number')).show()






from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

df.withColumn('odd_or_even', odd_even('number')).show()

