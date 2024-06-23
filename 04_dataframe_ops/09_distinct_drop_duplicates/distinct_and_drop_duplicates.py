from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

# Initialize a Spark session
spark = SparkSession.builder.appName("DistinctAndDropDuplicatesDemo").getOrCreate()

# Sample data with potential duplicates
data = [
    ("Alice", "Data Science", 1000),
    ("Bob", "Engineering", 1500),
    ("Alice", "Data Science", 1000),
    ("David", "HR", 750),
    ("Bob", "Engineering", 1500),
    ("Ella", "Marketing", 1200),
    ("Frank", "Marketing", 1200),
    ("Alice", "Data Science", 1000),
    ("Gina", "HR", 750),
    ("Bob", "Engineering", 2000)
]

# Define schema
columns = ["Name", "Department", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Register DataFrame as a SQL table
df.createOrReplaceTempView("employees")

# Exercises with solutions on handling distinct and drop duplicates
# Exercise 1: Remove all duplicate rows
df.distinct().show()
spark.sql("SELECT DISTINCT * FROM employees").show()

# Exercise 2: Remove duplicates based on specific columns (Name and Department)
df.dropDuplicates(["Name", "Department"]).show()
spark.sql("SELECT DISTINCT Name, Department, first(Salary) OVER (PARTITION BY Name, Department ORDER BY Salary) as Salary FROM employees").show()

# Exercise 3: Remove duplicates and keep the row with the highest salary
(df.withColumn("row_number", row_number().over(Window.partitionBy("Name", "Department").orderBy(col("Salary").desc())))
   .filter("row_number = 1")
   .drop("row_number")
   .show())
spark.sql("""
SELECT Name, Department, MAX(Salary) as Salary FROM employees GROUP BY Name, Department
""").show()

# Exercise 4: Show distinct departments only
df.select("Department").distinct().show()
spark.sql("SELECT DISTINCT Department FROM employees").show()

# Exercise 5: Count of distinct Names
print(df.select("Name").distinct().count())
spark.sql("SELECT COUNT(DISTINCT Name) FROM employees").show()

# Exercise 6: Count of distinct Department and Salary combinations
print(df.select("Department", "Salary").distinct().count())
spark.sql("SELECT COUNT(DISTINCT Department, Salary) FROM employees").show()

# Exercise 7: Remove duplicates and keep the lowest salary for each name
(df.withColumn("row_number", row_number().over(Window.partitionBy("Name").orderBy("Salary")))
   .filter("row_number = 1")
   .drop("row_number")
   .show())
spark.sql("""
SELECT Name, first(Department) OVER (PARTITION BY Name ORDER BY Salary) as Department, MIN(Salary) as Salary FROM employees GROUP BY Name
""").show()

# Exercise 8: Remove duplicates for all columns except Salary
df.dropDuplicates(["Name", "Department"]).show()
spark.sql("""
SELECT Name, Department, first(Salary) OVER (PARTITION BY Name, Department ORDER BY Salary) as Salary FROM employees
""").show()

# Exercise 9: Show distinct salaries greater than 1000
df.select("Salary").distinct().filter("Salary > 1000").show()
spark.sql("SELECT DISTINCT Salary FROM employees WHERE Salary > 1000").show()

# Exercise 10: Get all distinct combinations of Name and Department
df.select("Name", "Department").distinct().show()
spark.sql("SELECT DISTINCT Name, Department FROM employees").show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Basic Duplicate Removal
#   1. Remove all duplicate rows: How do you completely remove duplicate rows from a DataFrame?
#   2. Remove duplicates based on specific columns (Name and Department): How can you remove duplicates based on a combination of specific columns?

# B. Conditional Duplicate Removal
#   3. Remove duplicates and keep the row with the highest salary: How do you remove duplicates while keeping the row with the highest salary for each group?
#   4. Show distinct departments only: What is the method to display only the unique departments from a DataFrame?

# C. Distinct Count Operations
#   5. Count of distinct Names: How can you find the count of unique names in the DataFrame?
#   6. Count of distinct Department and Salary combinations: How do you count the unique combinations of Department and Salary in the DataFrame?

# D. Advanced Duplicate Removal
#   7. Remove duplicates and keep the lowest salary for each name: How can you remove duplicates while retaining the entry with the lowest salary for each name?
#   8. Remove duplicates for all columns except Salary: How can you drop duplicates considering all columns except Salary?

# E. Filtering and Distinct Values
#   9. Show distinct salaries greater than 1000: How do you display distinct salaries that are greater than 1000?
#   10. Get all distinct combinations of Name and Department: What method would you use to extract all unique combinations of Name and Department from the DataFrame?

# Each question targets different aspects of using distinct and dropDuplicates functions in PySpark,
# allowing for the effective cleaning of data by removing unwanted duplicate entries or extracting unique data for further analysis.
