from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinOperationsDemo").getOrCreate()

# Load the data into DataFrames
df_employees = spark.read.csv("data/Employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("data/Departments.csv", header=True, inferSchema=True)

# Register DataFrames as SQL tables
df_employees.createOrReplaceTempView("employees")
df_departments.createOrReplaceTempView("departments")

# Inner Join
inner_join = df_employees.join(df_departments, "DepartmentID", "inner")
inner_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees INNER JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Outer (Full) Join
full_outer_join = df_employees.join(df_departments, "DepartmentID", "outer")
full_outer_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees FULL OUTER JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Join
left_join = df_employees.join(df_departments, "DepartmentID", "left")
left_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Right Join
right_join = df_employees.join(df_departments, "DepartmentID", "right")
right_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees RIGHT JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Anti Join
left_anti_join = df_employees.join(df_departments, "DepartmentID", "left_anti")
left_anti_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT ANTI JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Semi Join
left_semi_join = df_employees.join(df_departments, "DepartmentID", "left_semi")
left_semi_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT SEMI JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Self Join (demonstrating employees from the same department)
self_join = df_employees.alias("emp1").join(df_employees.alias("emp2"),
                                            col("emp1.DepartmentID") == col("emp2.DepartmentID"),
                                            "inner")
self_join.show()
# SQL Equivalent
spark.sql("""
SELECT emp1.*, emp2.*
FROM employees emp1
INNER JOIN employees emp2 ON emp1.DepartmentID = emp2.DepartmentID
""").show()

# Cross Join
cross_join = df_employees.crossJoin(df_departments)
cross_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees CROSS JOIN departments").show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Basic Join Types
#   1. Inner Join: How can you perform an inner join between the employees and departments DataFrames based on the "DepartmentID"?
#   2. Outer (Full) Join: What is the method to perform a full outer join on the "DepartmentID" between employees and departments?

# B. Specific Join Scenarios
#   3. Left Join: Demonstrate how to execute a left join between the employees and departments DataFrames, including all records from employees.
#   4. Right Join: How do you perform a right join, ensuring all records from the departments DataFrame are included even if there are no matches in employees?

# C. Specialized Join Types
#   5. Left Anti Join: What is a left anti join, and how can it be implemented to get the records from employees that do not match any department?
#   6. Left Semi Join: Explain a left semi join, which includes only the rows from employees that have a corresponding match in departments.

# D. Additional Join Operations
#   7. Self Join: How would you demonstrate a self join within the employees DataFrame to show employees from the same department?
#   8. Cross Join: Describe how to execute a cross join that combines every row of employees with every row of departments, disregarding any matching condition.

# Each question targets understanding different aspects of DataFrame join operations in PySpark,
# using both the DataFrame API and equivalent SQL queries to demonstrate how these operations can be achieved in SQL within a Spark environment.

