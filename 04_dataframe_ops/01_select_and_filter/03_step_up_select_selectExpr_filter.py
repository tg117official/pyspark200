# - Each withColumn exercise modifies the DataFrame by either adding a new
#   column or transforming an existing column, which is matched with an SQL query that performs the same operation.
#
# - The SQL equivalents use the SELECT statement with functions and expressions to
#   achieve the same results as withColumn.
#
# - Ensure the path to the CSV file is correctly set in the script and the CSV has
#   headers that match the column references used in the commands.

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("AdvancedEmployeeDataManipulation").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 11: Select Employees with a Salary Over $100,000
df.filter("salary > 100000").select("*").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE salary > 100000").show()

# Exercise 12: Select Employee Names and Departments Where the Department is Not 'Tech'
df.select("ename", "dept").filter("dept != 'Tech'").show()
# SQL Equivalent
spark.sql("SELECT ename, dept FROM employee WHERE dept != 'Tech'").show()

# Exercise 13: Use selectExpr to Calculate the Monthly Salary for Each Employee
df.selectExpr("ename", "salary/12 as monthly_salary").show()
# SQL Equivalent
spark.sql("SELECT ename, salary/12 as monthly_salary FROM employee").show()

# Exercise 14: Filter Employees Who Joined Before 2015 and Select Their Names and Joining Dates
df.filter("date_of_joining < '2015-01-01'").select("ename", "date_of_joining").show()
# SQL Equivalent
spark.sql("SELECT ename, date_of_joining FROM employee WHERE date_of_joining < '2015-01-01'").show()

# Exercise 15: Select Employees' Names with 'a' in Their Name
df.select("ename").filter("ename like '%a%'").show()
# SQL Equivalent
spark.sql("SELECT ename FROM employee WHERE ename LIKE '%a%'").show()

# Exercise 16: Use selectExpr to Show Employees' Name and a Boolean Column If Salary Is Above Average
df.selectExpr("ename", "salary > (SELECT avg(salary) FROM employee) as above_avg_salary").show()
# SQL Equivalent
spark.sql("SELECT ename, salary > (SELECT AVG(salary) FROM employee) as above_avg_salary FROM employee").show()

# Exercise 17: Filter Employees from the 'Finance' Department and Select All Columns
df.filter("dept = 'Finance'").select("*").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE dept = 'Finance'").show()

# Exercise 18: Use selectExpr to List Employees and the Year of Their Joining
df.selectExpr("ename", "year(date_of_joining) as joining_year").show()
# SQL Equivalent
spark.sql("SELECT ename, YEAR(date_of_joining) as joining_year FROM employee").show()

# Exercise 19: Filter for Employees Earning More Than $60,000 and in the 'HR' Department
df.filter("salary > 60000 AND dept = 'HR'").select("*").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE salary > 60000 AND dept = 'HR'").show()

# Exercise 20: Select Employee Names and Use selectExpr to Show if They Joined in the Last Decade
df.selectExpr("ename", "(year(current_date()) - year(date_of_joining) <= 10) as joined_last_decade").show()
# SQL Equivalent
spark.sql("SELECT ename, (YEAR(current_date()) - YEAR(date_of_joining) <= 10) as joined_last_decade FROM employee").show()

# Stop the Spark session
spark.stop()


# Questions:

# A. Basic Filtering and Selecting Exercises
#   11. Select Employees with a Salary Over $100,000: Demonstrate how to filter rows in a DataFrame and in SQL where salary is greater than 100,000.
#   12. Select Employee Names and Departments Where the Department is Not 'Tech': Show how to select and filter employee names and departments excluding those in the 'Tech' department.
#   13. Calculate the Monthly Salary for Each Employee: Use both DataFrame transformation and SQL to compute the monthly salary by dividing the annual salary by 12.

# B. More Advanced Filtering
#   14. Filter Employees Who Joined Before 2015 and Select Their Names and Joining Dates: Illustrate how to filter employees based on their joining date before the year 2015 and select relevant columns.
#   15. Select Employees' Names with 'a' in Their Name: Demonstrate how to filter employees whose names contain the letter 'a'.

# C. Conditional Expressions and Complex Filtering
#   16. Show Employees' Name and a Boolean Column If Salary Is Above Average: Use both DataFrame methods and SQL to add a boolean column indicating whether an employee's salary is above the average salary.
#   17. Filter Employees from the 'Finance' Department and Select All Columns: Explain how to filter employees who are in the 'Finance' department.
#   18. List Employees and the Year of Their Joining: Display how to extract the year from a date column both using DataFrame operations and SQL.

# D. Specific Conditional Logic
#   19. Filter for Employees Earning More Than $60,000 and in the 'HR' Department: Show filtering based on multiple conditions involving salary and department.
#   20. Show if Employees Joined in the Last Decade: Calculate using both DataFrame operations and SQL whether employees joined in the last decade based on the current date.

# Each exercise should demonstrate the application of DataFrame transformations and equivalent SQL queries to achieve the same results, facilitating learning and comparison between declarative and programmatic approaches in data manipulation.
