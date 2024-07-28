from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create UDF
concat_udf = udf(lambda x: f"Hello {x}", StringType())

# Create DataFrame
df = spark.createDataFrame([("John",), ("Jane",)], ["name"])

# Apply UDF
df.withColumn('greeting', concat_udf('name')).show()





from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def hello_name(name):
    return f"Hello {name}"

df.withColumn('greeting', hello_name('name')).show()