# Create RDD from employee.txt (csv) file, later Create DataFrame using  previously created RDD.

# Importing necessary libraries for Spark
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
empRDD = sc.textFile("data/employee.txt")

# Display the first three lines of the RDD to inspect the structure
initial_lines = empRDD.take(3)
print("Initial lines:", initial_lines)

# Extract the header from the RDD to separate it from the data
emp_header = empRDD.first()

# Filter out the header line from the RDD to isolate the data rows
empRDD1 = empRDD.filter(lambda x: x != emp_header)

# Display the first three lines of the filtered RDD to verify header removal
filtered_lines = empRDD1.take(3)
print("Filtered lines:", filtered_lines)

# Split each data row by comma to create lists of individual elements
empRDD2 = empRDD1.map(lambda x: x.split(','))

# Display the first three lines of the RDD after splitting
split_lines = empRDD2.take(3)
print("Split lines:", split_lines)

# Convert each element in the RDD to the appropriate data type and create tuples
empRDD3 = empRDD2.map(lambda x: (int(x[0]), x[1], x[2], float(x[3]), x[4]))

# Display the first three lines of the RDD after type conversion
typed_lines = empRDD3.take(3)
print("Typed lines:", typed_lines)

# Convert the RDD of tuples to a DataFrame
empDF = spark.createDataFrame(empRDD3, schema=["eid", "ename", "dept", "salary", "date_of_joining"])

# Show schema of empDF
empDF.printSchema()

# Show the DataFrame with its contents
empDF.show()

# Stop the Spark session
spark.stop()


