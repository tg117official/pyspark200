# Write a UDF that safely handles null values when performing string concatenation.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create UDF
def safe_concat(a, b):
    return f"{a or ''}{b or ''}"

safe_concat_udf = udf(safe_concat, StringType())

# Create DataFrame
df = spark.createDataFrame([(None, 'Spark'), ('Py', None)], ['a', 'b'])

# Apply UDF
df.withColumn('concatenated', safe_concat_udf('a', 'b')).show()










from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def safe_concat(a, b):
    return f"{a or ''}{b or ''}"

df.withColumn('concatenated', safe_concat('a', 'b')).show()








