from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "GroupByKeyLogLevelCount")

# Sample log data
log_data = [
    "INFO Server started",
    "WARN Disk space low",
    "ERROR Connection failed",
    "INFO Request processed",
    "ERROR Disk read failed",
    "WARN CPU usage high"
]

# Create RDD
logs_rdd = sc.parallelize(log_data)

# Filter relevant levels and map to (level, 1)
level_pairs = logs_rdd.flatMap(lambda line:
    [("ERROR", 1)] if "ERROR" in line else
    [("WARN", 1)] if "WARN" in line else [])

# Use groupByKey to group and count
grouped = level_pairs.groupByKey()
result = grouped.mapValues(lambda counts: sum(counts))

# Collect and print the result
for level, count in result.collect():
    print(f"{level}: {count}")
