from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFrameWriteModes").getOrCreate()

# Assuming df is your DataFrame loaded from any source
df = spark.read.csv("path_to_your_csv/Updated_Employee_Details.csv", header=True, inferSchema=True)

# Define a path for demonstration; replace this with your actual file system path
output_path = "data/output"

# Write Modes:
# 1. 'append' - Adds the new data to the existing data in the specified path
df.write.mode('append').csv(output_path + "/append_mode")
# Useful when you are adding data to an existing dataset, such as daily logs.

# 2. 'overwrite' - Overwrites the existing data with new data
df.write.mode('overwrite').csv(output_path + "/overwrite_mode")
# Use this mode carefully as it replaces all existing data with new data.

# 3. 'ignore' - Ignores the write operation if data already exists at the specified path
df.write.mode('ignore').csv(output_path + "/ignore_mode")
# This is useful when running jobs that should not modify existing data if it already exists.

# 4. 'error' (default) - Throws an error if data already exists at the specified path
df.write.mode('error').csv(output_path + "/error_mode")
# This mode ensures data is not accidentally overwritten.

# 5. 'errorifexists' - Similar to 'error', throws an error if data already exists
df.write.mode('errorifexists').csv(output_path + "/errorifexists_mode")
# Explicitly specifying this mode can improve readability of your code, making your intention clear.

# Additional examples to demonstrate file formats:
# Write to Parquet with overwrite mode
df.write.mode('overwrite').parquet(output_path + "/parquet_overwrite")

# Write to JSON with append mode
df.write.mode('append').json(output_path + "/json_append")

# Stop the Spark session
spark.stop()
