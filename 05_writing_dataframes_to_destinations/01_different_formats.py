from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("DataFrameExport") \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2,com.databricks:spark-xml_2.12:0.14.0") \
    .getOrCreate()

# Assuming df is your DataFrame loaded from any source
df = spark.read.csv("path_to_your_csv/Updated_Employee_Details.csv", header=True, inferSchema=True)

# Write to CSV
df.write.csv("file:///your_local_path/data/csv_output", header=True)

# Write to TSV
df.write.option("delimiter", "\t").csv("file:///your_local_path/data/tsv_output", header=True)

# Write to Pipe-separated file
df.write.option("delimiter", "|").csv("file:///your_local_path/data/pipe_output", header=True)

# Write to JSON
df.write.json("file:///your_local_path/data/json_output")

# Write to ORC
df.write.orc("file:///your_local_path/data/orc_output")

# Write to Parquet
df.write.parquet("file:///your_local_path/data/parquet_output")

# Write to Avro
df.write.format("avro").save("file:///your_local_path/data/avro_output")

# Write to XML
df.write.format("xml").option("rootTag", "employees").option("rowTag", "employee").save("file:///your_local_path/data/xml_output")

# Stop the Spark session
spark.stop()
