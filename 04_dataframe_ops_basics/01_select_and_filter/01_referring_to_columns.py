from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("ColumnReferenceDemo").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

# Demonstrating different ways to refer to columns

# 1. Using Column Names Directly as Strings
df.select("ename", "dept").show()
df.filter("salary > 80000").show()

# 2. Using the col() function
df.select(col("ename"), col("dept")).show()
df.filter(col("salary") > 80000).show()

# 3. Using DataFrame attribute-style access
df.select(df.ename, df.dept).show()
df.filter(df.salary > 80000).show()

# 4. Using selectExpr for SQL-like expressions
df.selectExpr("ename as employee_name", "dept as department").show()
df.selectExpr("avg(salary) as average_salary").show()

# 5. Using SQL directly by registering a temp view
df.createOrReplaceTempView("employees")
spark.sql("SELECT ename, dept FROM employees WHERE salary > 80000").show()
spark.sql("SELECT ename AS employee_name, dept AS department FROM employees").show()

# Stop the Spark session
spark.stop()
