# Each use case provides an example of how to apply sorting to your data, covering
# scenarios from basic to more complex sorting conditions.

# Both sort and orderBy can be used interchangeably in most cases, though orderBy is
# typically used more explicitly for clarity, especially in complex queries.

# SQL equivalents are provided to demonstrate how these operations can be mimicked in
# SQL queries within Spark SQL.

# Ensure the path to the CSV file is correctly set in the script and the CSV has headers
# that match the column references used in the commands.

# Adjust schema specifics and data types as necessary based on the actual content of your
# CSV file.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, rand, expr

# Initialize a Spark session
spark = SparkSession.builder.appName("SortingDemonstration").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("path_to_your_csv/Updated_Employee_Details.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Use Case 1: Sort by One Column in Ascending Order
df.sort("salary").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY salary").show()

# Use Case 2: Sort by One Column in Descending Order
df.sort(df.salary.desc()).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY salary DESC").show()

# Use Case 3: OrderBy with Ascending and Descending Together
df.orderBy(col("dept").asc(), col("salary").desc()).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY dept ASC, salary DESC").show()

# Use Case 4: Sort by Multiple Columns
df.sort("dept", "salary").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY dept, salary").show()

# Use Case 5: Using asc() and desc() in sort
df.sort(col("dept").asc(), col("ename").desc()).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY dept ASC, ename DESC").show()

# Use Case 6: OrderBy Using Expression
df.orderBy(expr("length(ename) desc")).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY LENGTH(ename) DESC").show()

# Use Case 7: Case-Insensitive Sorting
df.orderBy(asc("ename").asc_nulls_last()).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY ename ASC NULLS LAST").show()

# Use Case 8: Sorting with Null Values at the End
df.sort(col("date_of_joining").asc_nulls_last()).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee ORDER BY date_of_joining ASC NULLS LAST").show()

# Use Case 9: Sort Numerically on String Type Column by Casting
df.withColumn("eid_numeric", col("eid").cast("integer")).sort("eid_numeric").show()
# Note: SQL Equivalent would require a similar CAST operation

# Use Case 10: Random Sorting (for example purposes, not often useful)
df.orderBy(rand()).show()
# Note: SQL Equivalent would use ORDER BY RAND()

# Stop the Spark session
spark.stop()
