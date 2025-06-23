from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "CountActionsExample")

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

# Filter and count ERROR lines
error_count = logs_rdd.filter(lambda line: "ERROR" in line).count()

# Filter and count WARN lines
warn_count = logs_rdd.filter(lambda line: "WARN" in line).count()

# Print results
print("Number of ERROR lines:", error_count)
print("Number of WARN lines:", warn_count)
