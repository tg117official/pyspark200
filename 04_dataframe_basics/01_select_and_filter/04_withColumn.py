from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize a Spark session
spark = SparkSession.builder.appName("WithColumnTransformations").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("path_to_your_csv/Updated_Employee_Details.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("employee")

# Exercise 1: Add a New Column Showing Monthly Salary
df.withColumn("monthly_salary", df.salary / 12).show()
# SQL Equivalent
spark.sql("SELECT *, salary / 12 as monthly_salary FROM employee").show()

# Exercise 2: Update Department Names to Uppercase
df.withColumn("dept_uppercase", expr("upper(dept)")).show()
# SQL Equivalent
spark.sql("SELECT *, UPPER(dept) as dept_uppercase FROM employee").show()

# Exercise 3: Create a Boolean Column Checking if Salary is Above Average
average_salary = df.selectExpr("avg(salary)").first()[0]
df.withColumn("above_avg", df.salary > average_salary).show()
# SQL Equivalent
spark.sql("SELECT *, salary > (SELECT AVG(salary) FROM employee) as above_avg FROM employee").show()

# Exercise 4: Add Tenure Column Showing Years Since Joining
df.withColumn("tenure", expr("year(current_date()) - year(date_of_joining)")).show()
# SQL Equivalent
spark.sql("SELECT *, YEAR(current_date()) - YEAR(date_of_joining) as tenure FROM employee").show()

# Exercise 5: Create a Column to Categorize Salaries
df.withColumn("salary_category", expr("CASE WHEN salary > 100000 THEN 'High' WHEN salary > 50000 THEN 'Medium' ELSE 'Low' END")).show()
# SQL Equivalent
spark.sql("""
SELECT *, CASE 
            WHEN salary > 100000 THEN 'High' 
            WHEN salary > 50000 THEN 'Medium' 
            ELSE 'Low' 
          END as salary_category 
FROM employee
""").show()

# Exercise 6: Adjust Salary for a 10% Raise Across the Board
df.withColumn("adjusted_salary", df.salary * 1.1).show()
# SQL Equivalent
spark.sql("SELECT *, salary * 1.1 as adjusted_salary FROM employee").show()

# Exercise 7: Convert Date of Joining to 'YYYY-MM' Format
df.withColumn("joining_month_year", expr("date_format(date_of_joining, 'yyyy-MM')")).show()
# SQL Equivalent
spark.sql("SELECT *, date_format(date_of_joining, 'yyyy-MM') as joining_month_year FROM employee").show()

# Exercise 8: Create an Age Column Assuming All Employees are Born in 1985
df.withColumn("age", expr("year(current_date()) - 1985")).show()
# SQL Equivalent
spark.sql("SELECT *, YEAR(current_date()) - 1985 as age FROM employee").show()

# Exercise 9: Append a Suffix to Employee Names
df.withColumn("ename_suffix", expr("concat(ename, ' - Emp')")).show()
# SQL Equivalent
spark.sql("SELECT *, CONCAT(ename, ' - Emp') as ename_suffix FROM employee").show()

# Exercise 10: Normalize Salaries Between 0 and 1 Based on Min/Max Salary
min_salary, max_salary = df.selectExpr("min(salary)", "max(salary)").first()
df.withColumn("normalized_salary", (df.salary - min_salary) / (max_salary - min_salary)).show()
# SQL Equivalent
spark.sql("""
SELECT *, (salary - (SELECT MIN(salary) FROM employee)) / ((SELECT MAX(salary) FROM employee) - (SELECT MIN(salary) FROM employee)) as normalized_salary 
FROM employee
""").show()

# Stop the Spark session
spark.stop()
