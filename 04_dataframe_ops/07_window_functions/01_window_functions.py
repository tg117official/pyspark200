from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg, max, min, lead, lag, count

# Initialize a Spark session
spark = SparkSession.builder.appName("WindowFunctionsDemo").getOrCreate()

# Sample data to create the DataFrame
data = [
    ("North", "Sales", "Male", 60000, 100),
    ("South", "HR", "Female", 75000, 200),
    ("East", "Marketing", "Female", 72000, 150),
    ("West", "Sales", "Male", 50000, 250),
    ("North", "HR", "Male", 82000, 300),
    ("South", "Marketing", "Female", 68000, 120),
    ("East", "HR", "Male", 75000, 110),
    ("West", "Marketing", "Female", 54000, 210),
    ("North", "Sales", "Female", 63000, 180),
    ("South", "HR", "Male", 71000, 190)
]

# Define schema
columns = ["Region", "Department", "Gender", "Salary", "Sales"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Register DataFrame as a SQL table
df.createOrReplaceTempView("employees")

# Define a window specification
windowSpec = Window.partitionBy("Department").orderBy("Salary")

# Exercises with window functions
# Note: Each PySpark window function exercise is accompanied by its SQL equivalent.

# Exercise 1: Rank by Salary within each Department
df.withColumn("rank", rank().over(windowSpec)).show()
spark.sql("SELECT *, RANK() OVER (PARTITION BY Department ORDER BY Salary) as rank FROM employees").show()

# Exercise 2: Dense Rank by Salary within each Department
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
spark.sql("SELECT *, DENSE_RANK() OVER (PARTITION BY Department ORDER BY Salary) as dense_rank FROM employees").show()

# Exercise 3: Row Number by Salary within each Department
df.withColumn("row_number", row_number().over(windowSpec)).show()
spark.sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY Department ORDER BY Salary) as row_number FROM employees").show()

# Exercise 4: Cumulative Salary within each Department
df.withColumn("cumulative_salary", sum("Salary").over(windowSpec)).show()
spark.sql("SELECT *, SUM(Salary) OVER (PARTITION BY Department ORDER BY Salary) as cumulative_salary FROM employees").show()

# Exercise 5: Moving Average Salary within each Department
df.withColumn("avg_salary", avg("Salary").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, AVG(Salary) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as avg_salary FROM employees").show()

# Exercise 6: Maximum Salary within each Department up to current row
df.withColumn("max_salary_to_date", max("Salary").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, MAX(Salary) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_salary_to_date FROM employees").show()

# Exercise 7: Minimum Salary within each Department up to current row
df.withColumn("min_salary_to_date", min("Salary").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, MIN(Salary) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as min_salary_to_date FROM employees").show()

# Exercise 8: Lead Salary (next row's salary in the same department)
df.withColumn("next_salary", lead("Salary").over(windowSpec)).show()
spark.sql("SELECT *, LEAD(Salary) OVER (PARTITION BY Department ORDER BY Salary) as next_salary FROM employees").show()

# Exercise 9: Lag Salary (previous row's salary in the same department)
df.withColumn("prev_salary", lag("Salary").over(windowSpec)).show()
spark.sql("SELECT *, LAG(Salary) OVER (PARTITION BY Department ORDER BY Salary) as prev_salary FROM employees").show()

# Exercise 10: Difference between current salary and next salary within each department
df.withColumn("diff_next_salary", lead("Salary").over(windowSpec) - col("Salary")).show()
spark.sql("SELECT *, LEAD(Salary) OVER (PARTITION BY Department ORDER BY Salary) - Salary as diff_next_salary FROM employees").show()

# Exercise 11: Difference between current salary and previous salary within each department
df.withColumn("diff_prev_salary", col("Salary") - lag("Salary").over(windowSpec)).show()
spark.sql("SELECT *, Salary - LAG(Salary) OVER (PARTITION BY Department ORDER BY Salary) as diff_prev_salary FROM employees").show()

# Exercise 12: Percentage change from previous salary within each department
df.withColumn("pct_change_salary", (col("Salary") - lag("Salary").over(windowSpec)) / lag("Salary").over(windowSpec)).show()
spark.sql("SELECT *, (Salary - LAG(Salary) OVER (PARTITION BY Department ORDER BY Salary)) / LAG(Salary) OVER (PARTITION BY Department ORDER BY Salary) as pct_change_salary FROM employees").show()

# Exercise 13: Cumulative count of employees within each department
df.withColumn("cumulative_count", count("*").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, COUNT(*) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_count FROM employees").show()

# Exercise 14: Cumulative sum of sales within each department
df.withColumn("cumulative_sales", sum("Sales").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, SUM(Sales) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sales FROM employees").show()

# Exercise 15: Average sales within each department up to current row
df.withColumn("avg_sales_to_date", avg("Sales").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, AVG(Sales) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as avg_sales_to_date FROM employees").show()

# Exercise 16: Rank of sales within each department
df.withColumn("sales_rank", rank().over(Window.partitionBy("Department").orderBy("Sales"))).show()
spark.sql("SELECT *, RANK() OVER (PARTITION BY Department ORDER BY Sales) as sales_rank FROM employees").show()

# Exercise 17: Dense rank of sales within each department
df.withColumn("sales_dense_rank", dense_rank().over(Window.partitionBy("Department").orderBy("Sales"))).show()
spark.sql("SELECT *, DENSE_RANK() OVER (PARTITION BY Department ORDER BY Sales) as sales_dense_rank FROM employees").show()

# Exercise 18: Row number of sales within each department
df.withColumn("sales_row_number", row_number().over(Window.partitionBy("Department").orderBy("Sales"))).show()
spark.sql("SELECT *, ROW_NUMBER() OVER (PARTITION BY Department ORDER BY Sales) as sales_row_number FROM employees").show()

# Exercise 19: Maximum sales within each department up to current row
df.withColumn("max_sales_to_date", max("Sales").over(windowSpec.rowsBetween(Window.unboundedPreceding, Window.currentRow))).show()
spark.sql("SELECT *, MAX(Sales) OVER (PARTITION BY Department ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_sales_to_date FROM employees").show()

# Exercise 20: Lead of sales (next row's sales in the same department)
df.withColumn("next_sales", lead("Sales").over(windowSpec)).show()
spark.sql("SELECT *, LEAD(Sales) OVER (PARTITION BY Department ORDER BY Salary) as next_sales FROM employees").show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Ranking Functions
#   1. Rank by Salary within each Department: How can you apply the rank function to order employees by salary within each department?
#   2. Dense Rank by Salary within each Department: How do you use the dense_rank function to assign a rank to employees by salary without gaps within departments?
#   3. Row Number by Salary within each Department: What is the method for assigning a unique row number to each employee based on their salary within their department?

# B. Cumulative Aggregations
#   4. Cumulative Salary within each Department: How can you calculate the cumulative salary within each department sorted by salary?
#   5. Cumulative count of employees within each department: How do you count the number of employees cumulatively within each department?
#   6. Cumulative sum of sales within each department: What is the method for calculating the cumulative sales within departments?
#   7. Maximum Salary within each Department up to current row: How can you determine the maximum salary up to each row within department?
#   8. Minimum Salary within each Department up to current row: How can you compute the minimum salary up to each row within departments?

# C. Lag and Lead Operations
#   9. Lead Salary (next row's salary in the same department): How do you find the next row's salary within the same department?
#   10. Lag Salary (previous row's salary in the same department): How can you retrieve the previous row's salary within the same department?
#   11. Lead of sales (next row's sales in the same department): How do you determine the next row's sales within the same department?
#   12. Difference between current salary and next salary within each department: How do you calculate the difference between the current and next salary within departments?
#   13. Difference between current salary and previous salary within each department: How can you find the difference between current and previous salaries within each department?

# D. Percentage and Moving Average Calculations
#   14. Percentage change from previous salary within each department: What is the method to calculate the percentage change from the previous salary within each department?
#   15. Moving Average Salary within each Department: How do you compute a moving average of salaries within each department?
#   16. Average sales within each department up to current row: How can you determine the average sales up to each row within departments?

# E. Sales Specific Operations
#   17. Rank of sales within each department: How can you rank sales within departments?
#   18. Dense rank of sales within each department: How do you apply dense rank to sales within each department?
#   19. Row number of sales within each department: What is the method for assigning row numbers to sales records within each department?
#   20. Maximum sales within each department up to current row: How can you determine the maximum sales up to each row within departments?

# Each question targets the practical application of window functions in PySpark, demonstrating how to handle data within groups and across ordered datasets efficiently.

