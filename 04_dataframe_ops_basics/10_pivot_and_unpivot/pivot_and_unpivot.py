from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col

# Initialize a Spark session
spark = SparkSession.builder.appName("PivotUnpivotDemo").getOrCreate()

# Sample data
data = [
    ("North", "Apple", 10),
    ("North", "Apple", 15),
    ("North", "Banana", 20),
    ("South", "Apple", 15),
    ("South", "Banana", 25),
    ("East", "Apple", 5),
    ("East", "Banana", 30),
    ("West", "Apple", 20),
    ("West", "Banana", 10)
]

# Columns: Region, Fruit, Sales
df = spark.createDataFrame(data, ["Region", "Fruit", "Sales"])

# Register DataFrame as a SQL table
df.createOrReplaceTempView("sales")

# Pivot Exercises
# Exercise 1: Pivot the data to show total sales for each fruit by region
pivot_df = df.groupBy("Region").pivot("Fruit").sum("Sales")
pivot_df.show()
# SQL Equivalent (not directly supported in Spark SQL, conceptual only)
spark.sql("SELECT Region, SUM(CASE WHEN Fruit = 'Apple' THEN Sales ELSE 0 END) AS Apple, SUM(CASE WHEN Fruit = 'Banana' THEN Sales ELSE 0 END) AS Banana FROM sales GROUP BY Region").show()

# Exercise 2: Pivot with multiple aggregate functions
pivot_df2 = df.groupBy("Region").pivot("Fruit").agg(sum(col("Sales")), sum(expr("Sales * 1.1")).alias("SP"))
pivot_df2.show()
# SQL Equivalent is similar, adding multiple aggregates per case

# Unpivot Exercises
# Exercise 3: Unpivot the DataFrame back to its original form
unpivot_expr = "stack(2, 'Apple', Apple, 'Banana', Banana) as (Fruit, Sales)"
unpivot_df = pivot_df.select("Region", expr(unpivot_expr))
unpivot_df.show()

# stack(n, ...): This is a function used for unpivoting data in PySpark. n
# represents the number of key-value pairs (column pairs) that will be unpivoted
# into the resultant long format.
#
# In our case, n = 2 indicates there are two key-value pairs to consider for the
# stack.
# 'Apple', Apple, 'Banana', Banana: This portion defines the key-value pairs to
# unpivot.
#
# 'Apple' and 'Banana' are the keys, which will be literal column names in the
# unpivoted DataFrame.

# Apple and Banana are the current column names in the pivoted DataFrame, where
# their values in each row will be used as the values for the respective keys in
# the unpivoted DataFrame.
#
# as (Fruit, Sales): This defines the new column names for the unpivoted data:
#
# Fruit will hold the fruit names ('Apple', 'Banana').
# Sales will contain the corresponding sales values from the Apple and Banana columns of the pivoted DataFrame.

# SQL Equivalent (using UNPIVOT, not available in Spark SQL)
# Conceptual SQL:
# SELECT Region, Fruit, Sales FROM sales_table UNPIVOT (Sales FOR Fruit IN (Apple, Banana))

# Exercise 4: Dynamic Pivot using a list of values

fruits = [row.Fruit for row in df.select("Fruit").distinct().collect()]
pivot_df3 = df.groupBy("Region").pivot("Fruit", fruits).sum("Sales")
pivot_df3.show()

# Exercise 5: Pivot and add a total column
pivot_df4 = pivot_df.withColumn("Total", expr("Apple + Banana"))
pivot_df4.show()

# Exercise 6: Unpivot including a total column
unpivot_expr2 = "stack(3, 'Apple', Apple, 'Banana', Banana, 'Total', Total) as (Fruit, Sales)"
unpivot_df2 = pivot_df4.select("Region", expr(unpivot_expr2))
unpivot_df2.show()

# Exercise 7: Custom aggregation in pivot
pivot_df5 = df.groupBy("Region").pivot("Fruit").agg(sum(expr("Sales * 1.1")).alias("Adjusted_Sales"))
pivot_df5.show()

# Exercise 8: Pivot with filtering only specific fruits
pivot_df6 = df.filter(df.Fruit.isin(["Apple"])).groupBy("Region").pivot("Fruit").sum("Sales")
pivot_df6.show()

# Exercise 9: Pivot with ordered regions
pivot_df7 = df.groupBy("Region").pivot("Fruit").sum("Sales").orderBy("Region")
pivot_df7.show()

# Exercise 10: Pivot and compute a percentage of total sales by fruit
total_sales = df.groupBy("Fruit").sum("Sales").withColumnRenamed("sum(Sales)", "Total_Sales")
pivot_df8 = df.groupBy("Region").pivot("Fruit").sum("Sales").join(total_sales, "Fruit")
pivot_df8 = pivot_df8.withColumn("Percentage", expr("sum(Sales) / Total_Sales * 100"))
pivot_df8.show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Pivot Operations
#   1. Pivot the data to show total sales for each fruit by region: How can you transform the DataFrame to show sales sums pivoted by fruit types across regions?
#   2. Pivot with multiple aggregate functions: How do you pivot data using multiple aggregate functions on the sales data?
#   3. Dynamic Pivot using a list of values: How can you dynamically pivot the data using an externally defined list of fruits?
#   4. Pivot and add a total column: After pivoting the data by fruit, how can you add a column that sums these pivoted values?
#   5. Custom aggregation in pivot: How do you apply a custom aggregation function, like a modified sum, during a pivot operation?
#   6. Pivot with filtering only specific fruits: How do you pivot data for only selected fruits, such as 'Apple'?
#   7. Pivot with ordered regions: How can you pivot data and ensure the results are ordered by regions?
#   8. Pivot and compute a percentage of total sales by fruit: After pivoting, how can you compute and display what percentage of total sales each fruit makes up per region?

# B. Unpivot Operations
#   3. Unpivot the DataFrame back to its original form: After pivoting, how can you transform it back to its original format with 'Fruit' and 'Sales' columns?
#   6. Unpivot including a total column: How can you include additional calculated totals when unpivoting the data back?

# Each question targets different aspects of pivoting and unpivoting in PySpark,
# allowing for flexible reshaping of data frames to summarize or detail data based on analytical needs.

