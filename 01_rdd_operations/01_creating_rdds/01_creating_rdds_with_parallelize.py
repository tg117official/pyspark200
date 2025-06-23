# You can create an RDD by parallelizing an existing list or collection in your driver
# program. This method uses the parallelize() function of the Spark context.

from pyspark import SparkContext

sc = SparkContext("local", "ParallelizeExample")
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
print(rdd.collect())
input("Press Enter to Exit : ")
