from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "RDDJoinExample")

# Small dataset: country codes
country_data = [
    ("US", "United States"),
    ("IN", "India"),
    ("FR", "France")
]

# Large dataset: users
users = [
    (1, "Alice", "US"),
    (2, "Bob", "IN"),
    (3, "Charlie", "FR"),
    (4, "David", "US")
]

# Parallelize both datasets
countries_rdd = sc.parallelize(country_data)  # RDD of (country_code, country_name)
users_rdd = sc.parallelize(users)             # RDD of (user_id, name, country_code)

# Reformat users RDD to: (country_code, (user_id, name))
users_kv = users_rdd.map(lambda x: (x[2], (x[0], x[1])))

# Now perform a standard RDD join: (country_code, ((user_id, name), country_name))
joined_rdd = users_kv.join(countries_rdd)

# Optionally format the result
final_rdd = joined_rdd.map(lambda x: (x[1][0][0], x[1][0][1], x[0], x[1][1]))

# Collect and print the result
for row in final_rdd.collect():
    print(row)
