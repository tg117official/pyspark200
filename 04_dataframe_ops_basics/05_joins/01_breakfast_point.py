from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinOperationsComplete").getOrCreate()

# Load the data into DataFrames
df_breakfast_orders = spark.read.option("header", "true").csv("data/breakfast_orders.txt")
df_token_details = spark.read.option("header", "true").csv("data/token_details.txt")

# Register DataFrames as SQL tables for SQL queries
df_breakfast_orders.createOrReplaceTempView("breakfast_orders")
df_token_details.createOrReplaceTempView("token_details")

# Inner Join
inner_join = df_breakfast_orders.join(df_token_details,df_breakfast_orders.token_color == df_token_details.color,"inner")
inner_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders INNER JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Full Outer Join
full_outer_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "outer")
full_outer_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders FULL OUTER JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Join
left_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left")
left_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Right Join
right_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "right")
right_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders RIGHT JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Semi Join
left_semi_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left_semi")
left_semi_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT SEMI JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Anti Join
left_anti_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left_anti")
left_anti_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT ANTI JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Cross Join
cross_join = df_breakfast_orders.crossJoin(df_token_details)
cross_join.show(50)
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders CROSS JOIN token_details").show()

# Stop the Spark session
spark.stop()


# Questions:

# A. Basic Join Operations
#   1. Inner Join: How can you perform an inner join between two DataFrames based on matching 'token_color' in one and 'color' in the other?
#   2. Full Outer Join: What is the method to perform a full outer join between two DataFrames based on the same color matching criteria?

# B. Specific Types of Joins
#   3. Left Join: How do you execute a left join to include all records from the left DataFrame and matched records from the right DataFrame based on color?
#   4. Right Join: Demonstrate how to perform a right join, ensuring all records from the right DataFrame and matched records from the left DataFrame are included.

# C. Less Common Joins
#   5. Left Semi Join: What is a left semi join, and how can it be implemented to include only the rows from the left DataFrame that have corresponding matches in the right DataFrame?
#   6. Left Anti Join: How do you perform a left anti join to obtain only the rows from the left DataFrame that do not have corresponding matches in the right DataFrame?

# D. Unconditional Join
#   7. Cross Join: Describe how to execute a cross join that combines every row of the left DataFrame with every row of the right DataFrame.

# Each question aims to explore different aspects of DataFrame join operations in PySpark, utilizing both the DataFrame API and equivalent SQL queries to demonstrate how these operations can be mirrored in SQL within a Spark environment.

