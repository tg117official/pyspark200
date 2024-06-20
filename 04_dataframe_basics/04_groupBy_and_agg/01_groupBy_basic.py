# Each exercise shows how to use groupBy along with agg to perform various aggregate
# functions, which are crucial for summarizing and analyzing large datasets.

# SQL equivalents help illustrate how these operations map to traditional SQL queries,
# aiding those familiar with SQL to understand how to perform similar tasks in PySpark.

# Ensure the path to the CSV file is correctly set in the script and that your Spark
# session is configured correctly to run without errors.

# Modify data types and schema details as necessary based on the actual contents of your
# data.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, max, min, count, expr, countDistinct

# Initialize a Spark session
spark = SparkSession.builder.appName("GroupByAndAggExercises").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 1: Calculate Average Salary by Department
df.groupBy("dept").agg(avg("salary").alias("avg_salary")).show()
# SQL Equivalent
spark.sql("SELECT dept, AVG(salary) AS avg_salary FROM employee GROUP BY dept").show()

# Exercise 2: Find Maximum and Minimum Salaries in Each Department
df.groupBy("dept").agg(max("salary").alias("max_salary"), min("salary").alias("min_salary")).show()
# SQL Equivalent
spark.sql("SELECT dept, MAX(salary) AS max_salary, MIN(salary) AS min_salary FROM employee GROUP BY dept").show()

# Exercise 3: Count the Number of Employees in Each Department
df.groupBy("dept").agg(count("*").alias("num_employees")).show()
# SQL Equivalent
spark.sql("SELECT dept, COUNT(*) AS num_employees FROM employee GROUP BY dept").show()

# Exercise 4: Find the Total Salary Expenditure by Department
df.groupBy("dept").agg(sum("salary").alias("total_salary")).show()
# SQL Equivalent
spark.sql("SELECT dept, SUM(salary) AS total_salary FROM employee GROUP BY dept").show()

# Exercise 5: Calculate Average, Maximum, and Minimum Salary in the Whole Organization
df.agg(avg("salary").alias("average_salary"), max("salary").alias("max_salary"), min("salary").alias("min_salary")).show()
# SQL Equivalent
spark.sql("SELECT AVG(salary) AS average_salary, MAX(salary) AS max_salary, MIN(salary) AS min_salary FROM employee").show()

# Exercise 6: List Departments with Average Salary Above 80,000
df.groupBy("dept").agg(avg("salary").alias("avg_salary")).filter("avg_salary > 80000").show()
# SQL Equivalent
spark.sql("SELECT dept, AVG(salary) AS avg_salary FROM employee GROUP BY dept HAVING AVG(salary) > 80000").show()

# Exercise 7: Count Distinct Employee Names in Each Department
df.groupBy("dept").agg(countDistinct("ename").alias("distinct_names")).show()
# SQL Equivalent
spark.sql("SELECT dept, COUNT(DISTINCT ename) AS distinct_names FROM employee GROUP BY dept").show()

# Exercise 8: Find Oldest Joining Date by Department
df.groupBy("dept").agg(min("date_of_joining").alias("oldest_joining")).show()
# SQL Equivalent
spark.sql("SELECT dept, MIN(date_of_joining) AS oldest_joining FROM employee GROUP BY dept").show()

# Exercise 9: Compute Total Salary and Number of Employees in Each Department
df.groupBy("dept").agg(sum("salary").alias("total_salary"), count("*").alias("num_employees")).show()
# SQL Equivalent
spark.sql("SELECT dept, SUM(salary) AS total_salary, COUNT(*) AS num_employees FROM employee GROUP BY dept").show()

# Exercise 10: Find the Highest Salary for Each Year of Joining
df.withColumn("year_of_joining", expr("year(date_of_joining)")).groupBy("year_of_joining").agg(max("salary").alias("highest_salary")).show()
# SQL Equivalent
spark.sql("SELECT YEAR(date_of_joining) AS year_of_joining, MAX(salary) AS highest_salary FROM employee GROUP BY YEAR(date_of_joining)").show()

# Stop the Spark session
spark.stop()
