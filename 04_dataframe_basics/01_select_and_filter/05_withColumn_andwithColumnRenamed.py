# Each exercise uses withColumn to create or modify a column based on some
# operation or transformation, followed by withColumnRenamed to rename the
# new or altered column.

# The SQL equivalents accomplish the same tasks using aliases in the SELECT
# statement to rename the columns immediately, providing a clean and direct comparison.

# Make sure the path to the CSV file is correct and that the file is properly
# formatted with headers. This script assumes basic familiarity with PySpark and
# SQL functions.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize a Spark session
spark = SparkSession.builder.appName("WithColumnAndRename").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("path_to_your_csv/Updated_Employee_Details.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 1: Calculate Monthly Salary, then Rename the Column to 'Monthly_Income'
df = df.withColumn("monthly_salary", col("salary") / 12).withColumnRenamed("monthly_salary", "monthly_income")
df.show()
# SQL Equivalent
spark.sql("SELECT *, salary / 12 as monthly_income FROM employee").show()

# Exercise 2: Convert Joining Date to Year, then Rename the Year Column to 'Year_Joined'
df = df.withColumn("join_year", expr("year(date_of_joining)")).withColumnRenamed("join_year", "year_joined")
df.show()
# SQL Equivalent
spark.sql("SELECT *, YEAR(date_of_joining) as year_joined FROM employee").show()

# Exercise 3: Add a Column for Length of Employee Name, then Rename it to 'Name_Length'
df = df.withColumn("name_length", expr("length(ename)")).withColumnRenamed("name_length", "name_length")
df.show()
# SQL Equivalent
spark.sql("SELECT *, LENGTH(ename) as name_length FROM employee").show()

# Exercise 4: Create an Age Column Assuming All Employees are Born in 1985, Rename it to 'Employee_Age'
df = df.withColumn("age", expr("year(current_date()) - 1985")).withColumnRenamed("age", "employee_age")
df.show()
# SQL Equivalent
spark.sql("SELECT *, YEAR(current_date()) - 1985 as employee_age FROM employee").show()

# Exercise 5: Normalize Salary and then Rename the Column to 'Salary_Normalized'
min_salary, max_salary = df.selectExpr("min(salary)", "max(salary)").first()
df = df.withColumn("normalized_salary", (col("salary") - min_salary) / (max_salary - min_salary))
df = df.withColumnRenamed("normalized_salary", "salary_normalized")
df.show()
# SQL Equivalent
spark.sql("""
SELECT *, (salary - (SELECT MIN(salary) FROM employee)) / ((SELECT MAX(salary) FROM employee) - (SELECT MIN(salary) FROM employee)) as salary_normalized 
FROM employee
""").show()

# Stop the Spark session
spark.stop()
