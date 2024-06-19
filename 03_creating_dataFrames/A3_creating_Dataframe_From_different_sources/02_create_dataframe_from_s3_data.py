from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("S3ReadExample") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<ACCESS_SECRET>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.option("header","true").option("inferSchema","true").csv("s3a://pst-18april/employee_csv/employee.txt")
df.show()

spark.stop()

# Execute Following Command in terminal :
# spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.253
# \path\to\02_create_dataframe_from_s3_data.py
