from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("EmployeeDataAnalysis").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 1: Select All Columns
df.show()
# Alternative using select with "*"
df.select("*").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee").show()


# Exercise 2: Select Specific Columns
df.select("ename", "dept").show()
# Alternative using selectExpr
df.selectExpr("ename", "dept").show()
# SQL Equivalent
spark.sql("SELECT ename, dept FROM employee").show()


# Exercise 3: Select with Column Renaming
df.select(df.salary.alias("monthly_salary")).show()
df.select(col("salary").alias("montly_salary")).show()
# Alternative using selectExpr
df.selectExpr("salary as monthly_salary").show()
# SQL Equivalent
spark.sql("SELECT salary AS monthly_salary FROM employee").show()


# Exercise 4: Select and Perform an Operation
df.select((df.salary / 12).alias("monthly_salary")).show()
df.select((col("salary") / 12).alias("monthly_salary")).show()
# Alternative using selectExpr for calculation
df.selectExpr("salary / 12 as monthly_salary").show()
# SQL Equivalent
spark.sql("SELECT salary / 12 AS monthly_salary FROM employee").show()


# Exercise 5: Filter by One Condition
df.filter(df.dept == "Tech").show()
# Alternative using where
df.where(df.dept == "Tech").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE dept = 'Tech'").show()


# Exercise 6: Filter by Multiple Conditions
df.filter((df.dept == "Finance") & (df.salary > 100000)).show()
# Alternative using where
df.where((df.dept == "Finance") & (df.salary > 100000)).show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE dept = 'Finance' AND salary > 100000").show()


# Exercise 7: Date Filter
df.filter(df.date_of_joining > "2015-01-01").show()
# Alternative using where
df.where(df.date_of_joining > "2015-01-01").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE date_of_joining > '2015-01-01'").show()


# Exercise 8: Select and Filter Combination
df.select("eid", "ename").filter(df.dept == "HR").show()
# Alternative using where
df.select("eid", "ename").where(df.dept == "HR").show()
# SQL Equivalent
spark.sql("SELECT eid, ename FROM employee WHERE dept = 'HR'").show()


# Exercise 9: Multiple Conditions with Select
df.select("ename", "dept", "date_of_joining").filter((df.salary > 75000) & (df.dept == "Marketing")).show()
# Alternative using where with selectExpr
df.selectExpr("ename", "dept", "date_of_joining").where("salary > 75000 AND dept = 'Marketing'").show()
# SQL Equivalent
spark.sql("SELECT ename, dept, date_of_joining FROM employee WHERE salary > 75000 AND dept = 'Marketing'").show()


# Exercise 10: Complex Combination
df.select("ename", "salary").filter((df.date_of_joining < "2010-01-01") & (df.salary < 80000)).show()
# Alternative using where with selectExpr
df.selectExpr("ename", "salary").where("date_of_joining < '2010-01-01' AND salary < 80000").show()
# SQL Equivalent
spark.sql("SELECT ename, salary FROM employee WHERE date_of_joining < '2010-01-01' AND salary < 80000").show()

# Stop the Spark session
spark.stop()


# Questions :
# A. Select Exercises
# 	1. Select All Columns: Write a PySpark code to load the CSV and select all columns from the DataFrame.
# 	2. Select Specific Columns: Select only the `ename` and `dept` columns from the DataFrame.
# 	3. Select with Column Renaming: Select the `salary` column and rename it to `monthly_salary`.
# 	4. Select and Perform an Operation: Select the `salary` column and convert its values from annual to monthly by dividing by 12.
#
# B. Filter Exercises
# 	5. Filter by One Condition: Filter the DataFrame to include only employees from the "Tech" department.
# 	6. Filter by Multiple Conditions: Find employees from the "Finance" department who have a salary greater than 100,000.
# 	7. Date Filter: Filter employees who joined after January 1, 2015.
#
# C. Combination of Select and Filter
# 	8. Select and Filter Combination: Select the `eid` and `ename` of all employees in the "HR" department.
# 	9. Multiple Conditions with Select: Select `ename`, `dept`, and `date_of_joining` for employees who earn more than 75,000 and work in the "Marketing" department.
# 	10. Complex Combination: Select `ename` and `salary`, and filter for employees who joined before 2010 and have a salary less than 80,000.
