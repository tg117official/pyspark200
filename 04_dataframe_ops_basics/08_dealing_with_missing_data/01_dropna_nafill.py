from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg, count

# Initialize a Spark session
spark = SparkSession.builder.appName("MissingDataHandling").getOrCreate()

# Sample data with missing values
data = [
    ("Alice", 25, None),
    ("Bob", None, 200),
    ("Charlie", 30, 300),
    ("David", 29, None),
    ("Ella", None, None),
    (None, 26, 150),
    ("Grace", 28, None),
    ("",25,600),
    (None, None, None),
    (None, None, None)
]

# Columns: Name, Age, Sales
df = spark.createDataFrame(data, ["Name", "Age", "Sales"])

# Register DataFrame as a SQL table
df.createOrReplaceTempView("people")

# Exercises with solutions on handling missing data
# Exercise 1: Drop any rows that contain missing data
df.na.drop().show()
spark.sql("SELECT * FROM people WHERE Age IS NOT NULL AND Sales IS NOT NULL").show()

# Exercise 2: Drop rows where all columns are missing
df.na.drop(how="all").show()
spark.sql("SELECT * FROM people WHERE NOT (Age IS NULL AND Sales IS NULL AND Name IS NULL)").show()

# Exercise 3: Drop rows where any column is missing
df.na.drop(how="any").show()
spark.sql("SELECT * FROM people WHERE Age IS NOT NULL AND Sales IS NOT NULL AND Name IS NOT NULL").show()

# Exercise 4: Drop rows where missing in specific columns
df.na.drop(subset=["Age"]).show()
spark.sql("SELECT * FROM people WHERE Age IS NOT NULL").show()

# Exercise 5: Fill all missing values with zeros
df.na.fill(0).show()
spark.sql("SELECT Name, COALESCE(Age, 0) AS Age, COALESCE(Sales, 0) AS Sales FROM people").show()

# Exercise 6: Fill missing values with specific values for each column
df.na.fill({"Age": 20, "Sales": 100}).show()
spark.sql("SELECT Name, COALESCE(Age, 20) AS Age, COALESCE(Sales, 100) AS Sales FROM people").show()

# Exercise 7: Fill missing values using the mean of the column
mean_age = df.select(avg("Age")).first()[0]
df.na.fill({"Age": mean_age}).show()
spark.sql(f"SELECT Name, COALESCE(Age, {mean_age}) AS Age, Sales FROM people").show()

# Exercise 8: Filter out rows with missing sales data
df.filter(col("Sales").isNotNull()).show()
spark.sql("SELECT * FROM people WHERE Sales IS NOT NULL").show()

# Exercise 9: Replace null values in 'Sales' with the average sales
avg_sales = df.select(avg("Sales")).first()[0]
df.na.fill({"Sales": avg_sales}).show()
spark.sql(f"SELECT Name, Age, COALESCE(Sales, {avg_sales}) AS Sales FROM people").show()

# Exercise 10: Use when() to replace nulls with a condition
df.withColumn("Sales", when(col("Sales").isNull(), 100).otherwise(col("Sales"))).show()
spark.sql("SELECT Name, Age, CASE WHEN Sales IS NULL THEN 100 ELSE Sales END AS Sales FROM people").show()

# Exercise 11: Count the number of nulls in each column
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
spark.sql("SELECT SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS Age, SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END) AS Sales FROM people").show()

# Exercise 12: Replace null values in text columns
df.na.fill({"Name": "Unknown"}).show()
spark.sql("SELECT COALESCE(Name, 'Unknown') AS Name, Age, Sales FROM people").show()

# Exercise 13: Replace null values before performing an aggregation
df.na.fill({"Sales": 0}).groupBy("Name").sum("Sales").show()
spark.sql("SELECT Name, SUM(COALESCE(Sales, 0)) AS Sales FROM people GROUP BY Name").show()

# Exercise 14: Use SQL to handle nulls in calculation
df.withColumn("Total", col("Sales") * col("Age")).na.fill({"Total": 0}).show()
spark.sql("SELECT Name, Age, Sales, COALESCE(Sales * Age, 0) AS Total FROM people").show()

# Exercise 15: Use when() with multiple conditions to handle nulls
df.withColumn("Status", when(col("Age") > 25, "Senior").otherwise("Junior")).na.fill({"Status": "Unknown"}).show()
spark.sql("SELECT Name, Age, Sales, CASE WHEN Age > 25 THEN 'Senior' WHEN Age IS NULL THEN 'Unknown' ELSE 'Junior' END AS Status FROM people").show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Dropping Rows with Missing Data
#   1. Drop any rows that contain missing data: How do you remove rows with any null values in the DataFrame?
#   2. Drop rows where all columns are missing: What is the method to drop rows where all column values are null?
#   3. Drop rows where any column is missing: How can you remove rows if any of their columns have null values?
#   4. Drop rows where missing in specific columns (e.g., Age): How do you drop rows based on null values in specific columns like Age?

# B. Filling Missing Values
#   5. Fill all missing values with zeros: How can you replace all null values in the DataFrame with zeros?
#   6. Fill missing values with specific values for each column: How do you specify different fill values for different columns?
#   7. Fill missing values using the mean of the column (e.g., Age): How can you replace null values in the 'Age' column with the mean age?

# C. Filtering and Replacing Nulls
#   8. Filter out rows with missing sales data: What method filters rows that have null values in the 'Sales' column?
#   9. Replace null values in 'Sales' with the average sales: How do you fill nulls in the 'Sales' column with the average sales value?

# D. Advanced Null Handling Techniques
#   10. Use when() to replace nulls with a condition (e.g., replace null in 'Sales' with 100): How do you use the when() function to replace null values under certain conditions?
#   11. Count the number of nulls in each column: How can you count the nulls in each column of the DataFrame?
#   12. Replace null values in text columns (e.g., Name with 'Unknown'): How do you replace null values in string-type columns?

# E. Nulls in Aggregations
#   13. Replace null values before performing an aggregation (e.g., sum of 'Sales'): How do you handle nulls in 'Sales' before aggregating to avoid calculation errors?
#   14. Use SQL to handle nulls in calculation (e.g., calculate total as Sales * Age, replace null results with 0): How do you ensure calculations with potential nulls are handled correctly?

# F. Handling Nulls with Conditional Logic
#   15. Use when() with multiple conditions to handle nulls (e.g., assign 'Senior' or 'Junior' based on Age, default to 'Unknown' if null): How do you apply multiple conditions with when() to handle nulls in calculations or assignments?

# Each question explores different strategies for dealing with missing data in PySpark, emphasizing both DataFrame operations and SQL queries to enhance data quality

