from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JSON Data Processing") \
    .getOrCreate()


# Exercise 1: Load a simple JSON file into a DataFrame
print("\nExercise 1: Load a simple JSON file into a DataFrame")
# Sample JSON: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
data1 = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
]
json_path_1 = "data/json_data/simple.json"
with open(json_path_1, 'w') as file:
    import json
    json.dump(data1, file)

df1 = spark.read.json(json_path_1)
df1.show()