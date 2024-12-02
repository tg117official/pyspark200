from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, json_tuple, \
                                  from_json, collect_list, \
                                  map_values, map_keys, to_json, \
                                  struct
from pyspark.sql.types import StructType, StructField, StringType, \
                              IntegerType, ArrayType, MapType

# Creating a Spark session
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



# Exercise 2: Load a nested JSON file and flatten it
print("\nExercise 2: Load a nested JSON file and flatten it")
# Sample JSON: [{"id": 1, "details": {"name": "Alice", "age": 25}}]
data2 = [
    {"id": 1, "details": {"name": "Alice", "age": 25}},
    {"id": 2, "details": {"name": "Bob", "age": 30}}
]
json_path_2 = "data/json_data/nested.json"
with open(json_path_2, 'w') as file:
    json.dump(data2, file)

df2 = spark.read.json(json_path_2)
df2.select(col("id"), col("details.name").alias("name"), col("details.age").alias("age")).show()



# Exercise 3: Load a JSON file with an array and explode the array
print("\nExercise 3: Load a JSON file with an array and explode the array")
# Sample JSON: [{"id": 1, "hobbies": ["reading", "swimming"]}]
data3 = [
    {"id": 1, "hobbies": ["reading", "swimming"]},
    {"id": 2, "hobbies": ["cycling", "hiking"]}
]
json_path_3 = "data/json_data/array.json"
with open(json_path_3, 'w') as file:
    json.dump(data3, file)

df3 = spark.read.json(json_path_3)
df3.select("id", explode(col("hobbies")).alias("hobby")).show()



# Exercise 4: Load a JSON file with schema inference
print("\nExercise 4: Load a JSON file with schema inference")
# Sample JSON: [{"id": 1, "price": 9.99}, {"id": 2, "price": 19.99}]
data4 = [
    {"id": 1, "price": 9.99},
    {"id": 2, "price": 19.99}
]
json_path_4 = "data/json_data/schema_inference.json"
with open(json_path_4, 'w') as file:
    json.dump(data4, file)

df4 = spark.read.option("inferSchema", "true").json(json_path_4)
df4.printSchema()
df4.show()



# Exercise 5: Load multiple JSON files into a single DataFrame
print("\nExercise 5: Load multiple JSON files into a single DataFrame")
data5a = [{"id": 1, "value": "A"}]
data5b = [{"id": 2, "value": "B"}]
json_path_5a = "data/json_data/multiple1.json"
json_path_5b = "data/json_data/multiple2.json"
with open(json_path_5a, 'w') as file:
    json.dump(data5a, file)
with open(json_path_5b, 'w') as file:
    json.dump(data5b, file)

df5 = spark.read.json(["data/json_data/multiple1.json", "data/json_data/multiple2.json"])
df5.show()



# Exercise 1: Handle JSON with deeply nested structures
print("\nExercise 1: Handle JSON with deeply nested structures")
# Sample JSON: [{"id": 1, "info": {"personal": {"name": "Alice", "age": 30}, "job": {"title": "Engineer", "department": "IT"}}}]
nested_json = [
    {"id": 1, "info": {"personal": {"name": "Alice", "age": 30}, "job": {"title": "Engineer", "department": "IT"}}},
    {"id": 2, "info": {"personal": {"name": "Bob", "age": 25}, "job": {"title": "Analyst", "department": "Finance"}}}
]
nested_path = "data/deep_nested.json"
with open(nested_path, 'w') as file:
    import json
    json.dump(nested_json, file)

df1 = spark.read.json(nested_path)
df1.select(
    col("id"),
    col("info.personal.name").alias("name"),
    col("info.personal.age").alias("age"),
    col("info.job.title").alias("job_title"),
    col("info.job.department").alias("department")
).show()



# Exercise 2: Parse JSON strings stored in a column
print("\nExercise 2: Parse JSON strings stored in a column")
# Sample JSON strings: [{"id": 1, "details": "{\"name\": \"Alice\", \"age\": 30}"}]
json_string_data = [
    {"id": 1, "details": "{\"name\": \"Alice\", \"age\": 30}"},
    {"id": 2, "details": "{\"name\": \"Bob\", \"age\": 25}"}
]
json_string_path = "data/json_string.json"
with open(json_string_path, 'w') as file:
    json.dump(json_string_data, file)

df2 = spark.read.json(json_string_path)
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df2.withColumn("parsed_details", from_json(col("details"), schema)).select("id", "parsed_details.*").show()



# Exercise 3: Extract multiple keys using json_tuple
print("\nExercise 3: Extract multiple keys using json_tuple")
# Sample JSON strings: [{"id": 1, "details": "{\"name\": \"Alice\", \"age\": 30, \"city\": \"New York\"}"}]
json_tuple_data = [
    {"id": 1, "details": "{\"name\": \"Alice\", \"age\": 30, \"city\": \"New York\"}"},
    {"id": 2, "details": "{\"name\": \"Bob\", \"age\": 25, \"city\": \"Los Angeles\"}"}
]
json_tuple_path = "data/json_tuple.json"
with open(json_tuple_path, 'w') as file:
    json.dump(json_tuple_data, file)

df3 = spark.read.json(json_tuple_path)
df3.select(
    col("id"),
    json_tuple(col("details"), "name", "age", "city").alias("name", "age", "city")
).show()



# Exercise 4: Handle JSON arrays of objects
print("\nExercise 4: Handle JSON arrays of objects")
# Sample JSON: [{"id": 1, "projects": [{"name": "Project A", "duration": "6 months"}, {"name": "Project B", "duration": "3 months"}]}]
array_json_data = [
    {"id": 1, "projects": [{"name": "Project A", "duration": "6 months"}, {"name": "Project B", "duration": "3 months"}]},
    {"id": 2, "projects": [{"name": "Project X", "duration": "12 months"}, {"name": "Project Y", "duration": "8 months"}]}
]
array_json_path = "data/array_objects.json"
with open(array_json_path, 'w') as file:
    json.dump(array_json_data, file)

df4 = spark.read.json(array_json_path)
df4.select("id", explode(col("projects")).alias("project")).select("id", col("project.name"), col("project.duration")).show()



# Exercise 5: Load JSON with a custom schema and handle missing fields
print("\nExercise 5: Load JSON with a custom schema and handle missing fields")
# Sample JSON: [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob"}]
missing_fields_data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob"}
]
missing_fields_path = "data/missing_fields.json"
with open(missing_fields_path, 'w') as file:
    json.dump(missing_fields_data, file)

custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df5 = spark.read.schema(custom_schema).json(missing_fields_path)
df5.na.fill({"age": 0}).show()



# Exercise 1: Handle multi-layer nested JSON with arrays and maps
print("\nExercise 1: Handle multi-layer nested JSON with arrays and maps")
# Sample JSON: [{"id": 1, "attributes": {"skills": ["Python", "Java"], "preferences": {"work": "remote", "team": "agile"}}}]
complex_json_data = [
    {"id": 1, "attributes": {"skills": ["Python", "Java"], "preferences": {"work": "remote", "team": "agile"}}},
    {"id": 2, "attributes": {"skills": ["Scala", "Spark"], "preferences": {"work": "on-site", "team": "scrum"}}}
]
complex_json_path = "data/multi_layer_nested.json"
with open(complex_json_path, 'w') as file:
    import json
    json.dump(complex_json_data, file)

df1 = spark.read.json(complex_json_path)
df1.select(
    col("id"),
    explode(col("attributes.skills")).alias("skill"),
    col("attributes.preferences.work").alias("work_preference"),
    col("attributes.preferences.team").alias("team_preference")
).show()



# Exercise 2: Transform nested JSON into a flat structure and reserialize into JSON
print("\nExercise 2: Transform nested JSON into a flat structure and reserialize into JSON")
nested_json_data = [
    {"id": 1, "info": {"name": "Alice", "age": 30, "address": {"city": "New York", "zip": "10001"}}},
    {"id": 2, "info": {"name": "Bob", "age": 25, "address": {"city": "Los Angeles", "zip": "90001"}}}
]
nested_json_path = "data/transform_and_serialize.json"
with open(nested_json_path, 'w') as file:
    json.dump(nested_json_data, file)

df2 = spark.read.json(nested_json_path)
flat_df = df2.select(
    col("id"),
    col("info.name").alias("name"),
    col("info.age").alias("age"),
    col("info.address.city").alias("city"),
    col("info.address.zip").alias("zip")
)
flat_df.show()
flat_df.withColumn("json_data", to_json(struct("id", "name", "age", "city", "zip"))).select("json_data").show(truncate=False)



# Exercise 3: Work with JSON containing hierarchical arrays
print("\nExercise 3: Work with JSON containing hierarchical arrays")
# Sample JSON: [{"id": 1, "teams": [{"name": "Team A", "members": ["Alice", "Bob"]}, {"name": "Team B", "members": ["Charlie", "David"]}]}]
hierarchical_json_data = [
    {"id": 1, "teams": [{"name": "Team A", "members": ["Alice", "Bob"]}, {"name": "Team B", "members": ["Charlie", "David"]}]},
    {"id": 2, "teams": [{"name": "Team X", "members": ["Eve", "Frank"]}, {"name": "Team Y", "members": ["Grace", "Heidi"]}]}
]
hierarchical_json_path = "data/hierarchical.json"
with open(hierarchical_json_path, 'w') as file:
    json.dump(hierarchical_json_data, file)

df3 = spark.read.json(hierarchical_json_path)
df3.select("id", explode(col("teams")).alias("team")).select(
    "id",
    col("team.name").alias("team_name"),
    explode(col("team.members")).alias("member")
).show()



# Exercise 4: Parse JSON fields containing dynamic keys using MapType
print("\nExercise 4: Parse JSON fields containing dynamic keys using MapType")
# Sample JSON: [{"id": 1, "metrics": {"clicks": 100, "impressions": 500}}, {"id": 2, "metrics": {"clicks": 200, "impressions": 800}}]
map_json_data = [
    {"id": 1, "metrics": {"clicks": 100, "impressions": 500}},
    {"id": 2, "metrics": {"clicks": 200, "impressions": 800}}
]
map_json_path = "data/dynamic_keys.json"
with open(map_json_path, 'w') as file:
    json.dump(map_json_data, file)

schema = StructType([
    StructField("id", IntegerType()),
    StructField("metrics", MapType(StringType(), IntegerType()))
])
df4 = spark.read.schema(schema).json(map_json_path)
df4.select("id", explode(col("metrics")).alias("metric", "value")).show()



# Exercise 5: Combine JSON data from multiple sources and deduplicate
print("\nExercise 5: Combine JSON data from multiple sources and deduplicate")
source1_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
source2_data = [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]
source1_path = "data/source1.json"
source2_path = "data/source2.json"
with open(source1_path, 'w') as file:
    json.dump(source1_data, file)
with open(source2_path, 'w') as file:
    json.dump(source2_data, file)

df_source1 = spark.read.json(source1_path)
df_source2 = spark.read.json(source2_path)
combined_df = df_source1.union(df_source2).dropDuplicates()
combined_df.show()


# Exercise 1: Process JSON with recursive structures
print("\nExercise 1: Process JSON with recursive structures")
# Sample JSON: [{"id": 1, "name": "Alice", "manager": {"id": 2, "name": "Bob"}}, {"id": 3, "name": "Charlie"}]
recursive_json_data = [
    {"id": 1, "name": "Alice", "manager": {"id": 2, "name": "Bob"}},
    {"id": 3, "name": "Charlie"}
]
recursive_json_path = "data/recursive.json"
with open(recursive_json_path, 'w') as file:
    import json
    json.dump(recursive_json_data, file)

df1 = spark.read.json(recursive_json_path)
df1.select(
    col("id").alias("employee_id"),
    col("name").alias("employee_name"),
    col("manager.id").alias("manager_id"),
    col("manager.name").alias("manager_name")
).show()



# Exercise 2: Work with JSON containing irregular nested arrays
print("\nExercise 2: Work with JSON containing irregular nested arrays")
# Sample JSON: [{"id": 1, "tasks": [{"name": "Task A", "hours": [1, 2, 3]}, {"name": "Task B", "hours": [4, 5]}]}]
irregular_array_json_data = [
    {"id": 1, "tasks": [{"name": "Task A", "hours": [1, 2, 3]}, {"name": "Task B", "hours": [4, 5]}]},
    {"id": 2, "tasks": [{"name": "Task X", "hours": [6]}, {"name": "Task Y", "hours": []}]}
]
irregular_array_json_path = "data/irregular_nested_arrays.json"
with open(irregular_array_json_path, 'w') as file:
    json.dump(irregular_array_json_data, file)

df2 = spark.read.json(irregular_array_json_path)
df2.select("id", explode(col("tasks")).alias("task")).select(
    "id",
    col("task.name").alias("task_name"),
    explode(col("task.hours")).alias("hour")
).show()



# Exercise 3: Transform JSON with dynamic nested structures
print("\nExercise 3: Transform JSON with dynamic nested structures")
# Sample JSON: [{"id": 1, "metrics": {"clicks": {"day1": 10, "day2": 20}, "impressions": {"day1": 100, "day2": 200}}}]
dynamic_json_data = [
    {"id": 1, "metrics": {"clicks": {"day1": 10, "day2": 20}, "impressions": {"day1": 100, "day2": 200}}},
    {"id": 2, "metrics": {"clicks": {"day1": 5, "day2": 15}, "impressions": {"day1": 50, "day2": 150}}}
]
dynamic_json_path = "data/dynamic_nested.json"
with open(dynamic_json_path, 'w') as file:
    json.dump(dynamic_json_data, file)

schema = StructType([
    StructField("id", IntegerType()),
    StructField("metrics", MapType(StringType(), MapType(StringType(), IntegerType())))
])
df3 = spark.read.schema(schema).json(dynamic_json_path)
df3.select(
    "id",
    explode(map_keys(col("metrics"))).alias("metric_type"),
    explode(map_values(col("metrics"))).alias("days")
).select(
    "id", "metric_type", explode(col("days")).alias("day", "value")
).show()



# Exercise 4: Flatten multi-level arrays and aggregate
print("\nExercise 4: Flatten multi-level arrays and aggregate")
# Sample JSON: [{"id": 1, "teams": [{"members": ["Alice", "Bob"]}, {"members": ["Charlie", "David"]}]}]
multi_level_array_data = [
    {"id": 1, "teams": [{"members": ["Alice", "Bob"]}, {"members": ["Charlie", "David"]}]},
    {"id": 2, "teams": [{"members": ["Eve", "Frank"]}, {"members": ["Grace", "Heidi"]}]}
]
multi_level_array_path = "data/multi_level_array.json"
with open(multi_level_array_path, 'w') as file:
    json.dump(multi_level_array_data, file)

df4 = spark.read.json(multi_level_array_path)
df4.select("id", explode(col("teams.members")).alias("members")).select(
    "id", explode(col("members")).alias("member")
).groupby("id").agg(collect_list("member").alias("all_members")).show(truncate=False)



# Exercise 5: Pivot JSON data and handle missing keys
print("\nExercise 5: Pivot JSON data and handle missing keys")
# Sample JSON: [{"id": 1, "sales": {"region1": 100, "region2": 200}}, {"id": 2, "sales": {"region2": 150}}]
pivot_json_data = [
    {"id": 1, "sales": {"region1": 100, "region2": 200}},
    {"id": 2, "sales": {"region2": 150}}
]
pivot_json_path = "data/pivot.json"
with open(pivot_json_path, 'w') as file:
    json.dump(pivot_json_data, file)

schema = StructType([
    StructField("id", IntegerType()),
    StructField("sales", MapType(StringType(), IntegerType()))
])
df5 = spark.read.schema(schema).json(pivot_json_path)
pivoted_df = df5.select("id", explode(col("sales")).alias("region", "sales"))
pivoted_df.groupBy("region").pivot("id").sum("sales").na.fill(0).show()



# File paths (replace with actual paths)
headers_path = "headers.json"
schema_path = "schema.json"
data_path = "data.json"

# Step 1: Load Headers JSON
with open(headers_path, 'r') as file:
    headers = json.load(file)

# Step 2: Load Schema JSON and construct StructType
with open(schema_path, 'r') as file:
    schema_json = json.load(file)

schema = StructType([
    StructField(field["name"], IntegerType() if field["type"] == "integer" else StringType(), field["nullable"])
    for field in schema_json["fields"]
])

# Step 3: Load Data JSON
raw_data_df = spark.read.json(data_path, schema=ArrayType(ArrayType(StringType())))
raw_data_df.show(truncate=False)

# Step 4: Create DataFrame using headers and data
df = raw_data_df.rdd.map(lambda row: tuple(row[0])).toDF(headers)
df = spark.createDataFrame(df.rdd, schema)
df.show()





headers_path = "data/json_data/header_2.json"
schema_path = "data/json_data/schema_2.json"
data_path = "data/json_data/data_2.json"

# Step 1: Load Headers JSON
with open(headers_path, 'r') as file:
    headers = json.load(file)

# Step 2: Load Schema JSON and construct StructType dynamically
def parse_schema(schema_dict):
    if schema_dict["type"] == "struct":
        return StructType([
            StructField(field["name"], parse_schema(field) if "fields" in field else (
                IntegerType() if field["type"] == "integer" else
                StringType() if field["type"] == "string" else
                ArrayType(StringType(), True)
            ), field["nullable"])
            for field in schema_dict["fields"]
        ])
    elif schema_dict["type"] == "array":
        return ArrayType(parse_schema(schema_dict["elementType"]))
    elif schema_dict["type"] == "integer":
        return IntegerType()
    elif schema_dict["type"] == "string":
        return StringType()

with open(schema_path, 'r') as file:
    schema_dict = json.load(file)

schema = parse_schema(schema_dict)

# Step 3: Load Data JSON
data = spark.read.json(data_path, schema=ArrayType(ArrayType(StringType())))
data.show(truncate=False)

# Step 4: Convert raw data to structured DataFrame
def map_row_to_nested_structure(row, headers):
    structured_row = {}
    for i, header in enumerate(headers):
        keys = header.split(".")
        current_level = structured_row
        for key in keys[:-1]:
            if key not in current_level:
                current_level[key] = {}
            current_level = current_level[key]
        current_level[keys[-1]] = row[i]
    return structured_row

rdd = data.rdd.map(lambda row: map_row_to_nested_structure(row[0], headers))
df = spark.createDataFrame(rdd, schema)

# Step 5: Show the final DataFrame
df.show(truncate=False)
df.printSchema()





from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import json

# Initialize SparkSession
spark = SparkSession.builder.appName("Optimized Dynamic DataFrame Creation").getOrCreate()

# File paths
headers_path = "data/json_data/header_2.json"
schema_path = "data/json_data/schema_2.json"
data_path = "data/json_data/data_2.json"


# Load Headers JSON
with open(headers_path, 'r') as file:
    headers = json.load(file)

# Load and Parse Schema JSON
def parse_schema(schema_dict):
    if "type" not in schema_dict:
        if "fields" in schema_dict:
            schema_dict["type"] = "struct"
        else:
            raise KeyError(f"Missing 'type' key in schema definition: {schema_dict}")
    if schema_dict["type"] == "struct":
        return StructType([
            StructField(
                field["name"],
                parse_schema(field) if "fields" in field else (
                    IntegerType() if field["type"] == "integer" else
                    StringType() if field["type"] == "string" else
                    ArrayType(StringType(), True)
                ),
                field.get("nullable", True)
            )
            for field in schema_dict["fields"]
        ])
    elif schema_dict["type"] == "array":
        return ArrayType(parse_schema(schema_dict["elementType"]))
    elif schema_dict["type"] == "integer":
        return IntegerType()
    elif schema_dict["type"] == "string":
        return StringType()
    else:
        raise ValueError(f"Unsupported type: {schema_dict['type']}")

with open(schema_path, 'r') as file:
    schema_dict = json.load(file)
schema = parse_schema(schema_dict)

# Load Data with Schema
df = spark.read.option("multiLine", True).schema(schema).json(data_path)

# Debugging: Check the loaded DataFrame before renaming
print("Original DataFrame:")
df.show(truncate=False)
df.printSchema()

# Correct Column Renaming for Nested Fields
print("\nDataFrame After Renaming Columns:")
for header in headers:
    parts = header.split(".")
    if len(parts) > 1:  # Nested field
        nested_field = ".".join(parts)
        alias_name = parts[-1]
        df = df.withColumnRenamed(nested_field, alias_name)

df.show(truncate=False)
df.printSchema()

# Stop SparkSession
spark.stop()

