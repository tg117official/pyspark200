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
inner_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "inner")
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
cross_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders CROSS JOIN token_details").show()

# Stop the Spark session
spark.stop()
