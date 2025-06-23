from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "BroadcastJoinRDD")

# Small dataset: country codes (to broadcast)
country_data = {
    "US": "United States",
    "IN": "India",
    "FR": "France"
}

# Broadcast the small dictionary
broadcast_countries = sc.broadcast(country_data)

# Large dataset: users with country codes
users = [
    (1, "Alice", "US"),
    (2, "Bob", "IN"),
    (3, "Charlie", "FR"),
    (4, "David", "US")
]

# Parallelize the large dataset
users_rdd = sc.parallelize(users)

# Perform join using the broadcast variable
def map_with_country_name(record):
    user_id, name, country_code = record
    country_name = broadcast_countries.value.get(country_code, "Unknown")
    return (user_id, name, country_code, country_name)

joined_rdd = users_rdd.map(map_with_country_name)

# Collect and print result
for row in joined_rdd.collect():
    print(row)
