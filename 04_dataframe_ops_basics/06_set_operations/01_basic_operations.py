from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFrameOperationsDemo").getOrCreate()

# Define sample data for two DataFrames
data1 = [Row(id=1, name="Alice", age=25),
         Row(id=2, name="Bob", age=30),
         Row(id=3, name="Charlie", age=35),
         Row(id=4, name="David", age=40),
         Row(id=5, name="Ella", age=45)]

data2 = [Row(id=3, name="Charlie", age=35),
         Row(id=4, name="David", age=40),
         Row(id=5, name="Ella", age=45),
         Row(id=6, name="Frank", age=50),
         Row(id=7, name="Grace", age=55)]

# Create DataFrames
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)

# Register DataFrames as SQL tables
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# Exercise 1: Union of df1 and df2
union_df = df1.union(df2)
union_df.show()
# SQL Equivalent
spark.sql("SELECT * FROM table1 UNION ALL SELECT * FROM table2").show()

# Exercise 2: Union Distinct of df1 and df2
union_distinct_df = df1.union(df2).distinct()
union_distinct_df.show()
# SQL Equivalent
spark.sql("SELECT * FROM table1 UNION SELECT * FROM table2").show()

# Exercise 3: Intersect of df1 and df2
intersect_df = df1.intersect(df2)
intersect_df.show()
# SQL Equivalent
spark.sql("SELECT * FROM table1 INTERSECT SELECT * FROM table2").show()

# Exercise 4: Subtract df1 from df2
subtract_df = df2.subtract(df1)
subtract_df.show()
# SQL Equivalent
spark.sql("SELECT * FROM table2 EXCEPT SELECT * FROM table1").show()

# Exercise 5: Subtract df2 from df1
subtract_df2 = df1.subtract(df2)
subtract_df2.show()
# SQL Equivalent
spark.sql("SELECT * FROM table1 EXCEPT SELECT * FROM table2").show()

# Exercise 6: Union of Intersect and Subtract (Combination)
combined_df = df1.intersect(df2).union(df1.subtract(df2))
combined_df.show()
# SQL Equivalent, combining results of INTERSECT and EXCEPT
spark.sql("(SELECT * FROM table1 INTERSECT SELECT * FROM table2) UNION ALL (SELECT * FROM table1 EXCEPT SELECT * FROM table2)").show()

# Exercise 7: Symmetric Difference (Union - Intersect)
symmetric_difference_df = df1.union(df2).subtract(df1.intersect(df2))
symmetric_difference_df.show()
# SQL Equivalent for symmetric difference
spark.sql("(SELECT * FROM table1 UNION ALL SELECT * FROM table2) EXCEPT (SELECT * FROM table1 INTERSECT SELECT * FROM table2)").show()

# Exercise 8: Duplicate Records After Union
duplicates_df = df1.union(df2).groupBy("id", "name", "age").count().filter("count > 1")
duplicates_df.show()
# SQL Equivalent for finding duplicates after union
spark.sql("""
SELECT id, name, age, COUNT(*) as count
FROM (SELECT * FROM table1 UNION ALL SELECT * FROM table2)
GROUP BY id, name, age
HAVING COUNT(*) > 1
""").show()

# Exercise 9: Add a column after Union to Identify Source DataFrame
source_df = df1.withColumn("source", lit("df1")).union(df2.withColumn("source", lit("df2")))
source_df.show()
# SQL Equivalent for adding source identification
spark.sql("""
SELECT *, 'df1' as source FROM table1
UNION ALL
SELECT *, 'df2' as source FROM table2
""").show()

# Exercise 10: Left Semi Join followed by a Union with Left Anti Join
semi_union_anti_df = df1.join(df2, "id", "left_semi").union(df1.join(df2, "id", "left_anti"))
semi_union_anti_df.show()
# SQL Equivalent combining LEFT SEMI JOIN and RIGHT ANTI JOIN
spark.sql("""
(SELECT table1.* FROM table1 JOIN table2 ON table1.id = table2.id)
UNION ALL
(SELECT table1.* FROM table1 RIGHT JOIN table2 ON table1.id = table2.id WHERE table1.id IS NULL)
""").show()

# Stop the Spark session
spark.stop()


# Questions:

# A. Basic Set Operations
#   1. Union of df1 and df2: How do you perform a union of two DataFrames to combine all rows from both, including duplicates?
#   2. Union Distinct of df1 and df2: How can you perform a union of two DataFrames that eliminates duplicate rows?

# B. Intersection and Difference
#   3. Intersect of df1 and df2: What method would you use to find rows that are common to both DataFrames?
#   4. Subtract df1 from df2: How do you determine rows in df2 that are not present in df1?
#   5. Subtract df2 from df1: Conversely, how do you find rows in df1 that are not in df2?

# C. Combining Multiple Operations
#   6. Union of Intersect and Subtract: How can you combine results of intersect and subtract operations between two DataFrames?
#   7. Symmetric Difference (Union - Intersect): What is the method to find rows in either df1 or df2 but not in both?

# D. Handling Duplicates
#   8. Duplicate Records After Union: After performing a union, how can you identify and display duplicate records?

# E. Additional Metadata in Unions
#   9. Add a column after Union to Identify Source DataFrame: How can you modify DataFrames before union to include a column indicating the source DataFrame?

# F. Complex Join Operations
#   10. Left Semi Join followed by a Union with Right Anti Join: How can you combine results of a left semi join and a right anti join between two DataFrames?

# Each question explores different aspects of manipulating DataFrames using both PySpark DataFrame operations and SQL queries, demonstrating how to achieve complex data transformations and analyses within a Spark environment.

