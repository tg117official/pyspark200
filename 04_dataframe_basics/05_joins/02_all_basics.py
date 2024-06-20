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
