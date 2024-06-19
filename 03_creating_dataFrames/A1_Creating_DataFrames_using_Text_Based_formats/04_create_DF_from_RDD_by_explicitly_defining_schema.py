# Create RDD from employee.txt (csv) file, later Create DataFrame using previously
# created RDD by explicitly defining schema.

# Importing necessary libraries for Spark, Spark SQL, and data types
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Creating a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Processing with RDDs") \
    .getOrCreate()

# Access SparkContext from SparkSession for RDD creation
sc = spark.sparkContext

# Read the text file containing employee data into an RDD
# Replace '<path to employee.txt>' with the actual path to your text file
empRDD = sc.textFile("file:///<path to employee.txt>")

# Display the first three lines of the RDD to inspect the structure
print("Initial lines:", empRDD.take(3))

# Extract the header from the RDD to separate it from the data
emp_header = empRDD.first()

# Filter out the header line from the RDD to isolate the data rows
empRDD1 = empRDD.filter(lambda x: x != emp_header)

# Display the first three lines of the filtered RDD to verify header removal
print("Filtered lines:", empRDD1.take(3))

# Split each data row by comma to create lists of individual elements
empRDD2 = empRDD1.map(lambda x: x.split(','))

# Display the first three lines of the RDD after splitting
print("Split lines:", empRDD2.take(3))

# Convert each element in the RDD to the appropriate data type and create tuples
empRDD3 = empRDD2.map(lambda x: (int(x[0]), x[1], x[2], float(x[3]), x[4]))

# Display the first three lines of the RDD after type conversion
print("Typed lines:", empRDD3.take(3))

# Define the schema for the employee DataFrame, specifying each column's name, data type, and nullability
emp_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("doj", StringType(), True)
])

# Create a DataFrame from the RDD of tuples using the defined schema
empDF = spark.createDataFrame(empRDD3, emp_schema)

# Display the schema of empDF
empDF.printSchema()

# Display the contents of the DataFrame
empDF.show()

# Stop the Spark session
spark.stop()
