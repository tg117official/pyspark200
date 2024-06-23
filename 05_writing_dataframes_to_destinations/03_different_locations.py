from pyspark.sql import SparkSession

# Initialize a Spark session with necessary dependencies
spark = SparkSession.builder.appName("DataFrameWriteDifferentOutputs")\
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3," 
            "org.apache.hadoop:hadoop-aws:3.3.4,"             
            "org.apache.spark:spark-avro_2.12:3.4.3,"          
            "org.apache.spark:spark-redshift_2.12:4.2.0,"    
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"  
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
    .enableHiveSupport() \
    .getOrCreate()

# Assuming df is your DataFrame loaded from any source
df = spark.read.csv("data/employee.txt", header=True, inferSchema=True)

# Local File System
df.write.csv("file:///path/to/local/file_system/output.csv")

# HDFS
df.write.csv("hdfs://path/to/hdfs/output.csv")

# AWS S3 (make sure to configure AWS credentials in your Spark context or environment variables)
df.write.csv("s3a://path/to/s3/bucket/output.csv")

# Hive (assuming Hive is set up and enabled in your Spark session)
df.write.saveAsTable("hive_table_name")

# Redshift (using the Redshift-Spark connector)
df.write \
    .format("io.github.spark_redshift_community.spark.redshift") \
    .option("url", "jdbc:redshift://<endpoint>:5439/database?user=username&password=password") \
    .option("dbtable", "redshift_table_name") \
    .option("tempdir", "s3a://path/to/temp/dir") \
    .mode("overwrite") \
    .save()

# Cassandra (specify keyspace and table)
df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "keyspace_name") \
    .option("table", "table_name") \
    .mode("append") \
    .save()

# MongoDB (specify the MongoDB URI and the collection)
df.write \
    .format("mongo") \
    .option("uri", "mongodb://username:password@host:port/database.collection") \
    .mode("append") \
    .save()

# Kafka (sending data as JSON to a Kafka topic)
df.selectExpr("CAST(eid AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("topic", "employee_topic") \
    .save()

# Stop the Spark session
spark.stop()
