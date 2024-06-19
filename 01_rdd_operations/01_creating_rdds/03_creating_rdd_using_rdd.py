# Every RDD transformation results in a new RDD. This means you can create a new RDD by
# applying transformations like map, filter, etc., to an existing RDD.

from pyspark import SparkContext

sc = SparkContext("local", "TransformationExample")
data = [1, 2, 3, 4, 5]
original_rdd = sc.parallelize(data)
transformed_rdd = original_rdd.map(lambda x: x * x)
print(transformed_rdd.collect())
