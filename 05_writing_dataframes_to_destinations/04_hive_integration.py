from pyspark.sql import SparkSession

# Initialize a Spark session with Hive support enabled
spark = SparkSession.builder \
    .appName("HiveOperations") \
    .enableHiveSupport() \
    .getOrCreate()

# Assuming df is your DataFrame loaded from any source
df = spark.read.csv("data/employee.txt", header=True, inferSchema=True)

# Create a new Hive table from DataFrame
df.write.saveAsTable("new_employee_table")

# Overwrite an existing Hive table
df.write.mode("overwrite").saveAsTable("existing_employee_table")

# Append data to an existing Hive table
df.write.mode("append").saveAsTable("existing_employee_table")

# Write data to a partitioned Hive table
df.write.partitionBy("dept").mode("overwrite").saveAsTable("partitioned_employee_table")

# Write data to a bucketed Hive table (Note: Bucketing is not supported in all storage formats)
df.write.bucketBy(10, "dept").sortBy("salary").saveAsTable("bucketed_employee_table", format="parquet")

# Stop the Spark session
spark.stop()