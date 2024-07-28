# Design a UDF to clean whitespace from a string column and apply it to a DataFrame.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create UDF
clean_whitespace_udf = udf(lambda x: x.strip(), StringType())

# Create DataFrame
df = spark.createDataFrame([("  hello  ",), ("  spark  ",)], ["dirty"])

# Apply UDF
df.withColumn('clean', clean_whitespace_udf('dirty')).show()






from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def clean_whitespace(x):
    return x.strip()

df.withColumn('clean', clean_whitespace('dirty')).show()




