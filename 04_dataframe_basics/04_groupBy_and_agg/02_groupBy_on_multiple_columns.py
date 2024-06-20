from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, count, countDistinct

# Initialize a Spark session
spark = SparkSession.builder.appName("MultiColumnGroupByWithSQL").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/region_data_emp.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("sales_data")

# Exercise 1: Average Salary by Region and Department
df.groupBy("Region", "Department").agg(avg("Salary").alias("Average_Salary")).show()
# SQL Equivalent
spark.sql("SELECT Region, Department, AVG(Salary) AS Average_Salary FROM sales_data GROUP BY Region, Department").show()

# Exercise 2: Total Sales by Region and Gender
df.groupBy("Region", "Gender").agg(sum("Sales").alias("Total_Sales")).show()
# SQL Equivalent
spark.sql("SELECT Region, Gender, SUM(Sales) AS Total_Sales FROM sales_data GROUP BY Region, Gender").show()

# Exercise 3: Maximum and Minimum Salary by Department and Gender
df.groupBy("Department", "Gender").agg(max("Salary").alias("Max_Salary"), min("Salary").alias("Min_Salary")).show()
# SQL Equivalent
spark.sql("SELECT Department, Gender, MAX(Salary) AS Max_Salary, MIN(Salary) AS Min_Salary FROM sales_data GROUP BY Department, Gender").show()

# Exercise 4: Count of Employees by Region, Department, and Gender
df.groupBy("Region", "Department", "Gender").agg(count("*").alias("Employee_Count")).show()
# SQL Equivalent
spark.sql("SELECT Region, Department, Gender, COUNT(*) AS Employee_Count FROM sales_data GROUP BY Region, Department, Gender").show()

# Exercise 5: Average Sales by Region and Department
df.groupBy("Region", "Department").agg(avg("Sales").alias("Average_Sales")).show()
# SQL Equivalent
spark.sql("SELECT Region, Department, AVG(Sales) AS Average_Sales FROM sales_data GROUP BY Region, Department").show()

# Exercise 6: Sum of Salaries by Gender and Department
df.groupBy("Gender", "Department").agg(sum("Salary").alias("Sum_Salaries")).show()
# SQL Equivalent
spark.sql("SELECT Gender, Department, SUM(Salary) AS Sum_Salaries FROM sales_data GROUP BY Gender, Department").show()

# Exercise 7: Highest Sales Recorded in Each Region by Gender
df.groupBy("Region", "Gender").agg(max("Sales").alias("Highest_Sales")).show()
# SQL Equivalent
spark.sql("SELECT Region, Gender, MAX(Sales) AS Highest_Sales FROM sales_data GROUP BY Region, Gender").show()

# Exercise 8: Count Distinct Departments in Each Region
df.groupBy("Region").agg(countDistinct("Department").alias("Distinct_Departments")).show()
# SQL Equivalent
spark.sql("SELECT Region, COUNT(DISTINCT Department) AS Distinct_Departments FROM sales_data GROUP BY Region").show()

# Exercise 9: Minimum Sales by Region and Gender
df.groupBy("Region", "Gender").agg(min("Sales").alias("Minimum_Sales")).show()
# SQL Equivalent
spark.sql("SELECT Region, Gender, MIN(Sales) AS Minimum_Sales FROM sales_data GROUP BY Region, Gender").show()

# Exercise 10: Total Number of Employees and Average Salary by Department and Gender
df.groupBy("Department", "Gender").agg(count("*").alias("Total_Employees"), avg("Salary").alias("Average_Salary")).show()
# SQL Equivalent
spark.sql("SELECT Department, Gender, COUNT(*) AS Total_Employees, AVG(Salary) AS Average_Salary FROM sales_data GROUP BY Department, Gender").show()

# Stop the Spark session
spark.stop()
