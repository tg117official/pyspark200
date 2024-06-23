from pyspark.sql import SparkSession

# Create a Spark session with the MySQL JDBC driver included
spark = SparkSession.builder \
    .appName("MySQLIntegration") \
    .config("spark.master", "local[*]") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.29") \
    .getOrCreate()

# Proceed with your JDBC operations...

# Properties dictionary containing JDBC connection parameters
props = {
    "driver": "com.mysql.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/<dbname>",
    "user": "root",
    "password": "root"
}

# Reading data from the MySQL database into a DataFrame using JDBC
empDF = spark.read.jdbc(url=props['url'], table='employee', properties=props)

# Displaying the top rows of the DataFrame to inspect the data
empDF.show()

# Printing the schema of the DataFrame to understand the data types and structure
empDF.printSchema()
