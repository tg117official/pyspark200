from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "AccumulatorMultipleExample")

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

# Create two accumulators
error_count = sc.accumulator(0)
warn_count = sc.accumulator(0)

# Function to update accumulators
def check_levels(line):
    global error_count, warn_count
    if "ERROR" in line:
        error_count += 1
    if "WARN" in line:
        warn_count += 1
    return line  # just a dummy return, as we're not transforming data here

# Trigger the function by using an action (e.g., foreach)
logs_rdd.foreach(check_levels)



# Access the accumulator values from the driver
print("Number of ERROR lines:", error_count.value)
print("Number of WARN lines:", warn_count.value)

input()
