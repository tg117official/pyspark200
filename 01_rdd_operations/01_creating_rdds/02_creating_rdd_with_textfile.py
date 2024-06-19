# You can create an RDD from an external dataset such as a local file system or HDFS using
# the textFile() method, which reads files as RDDs of text lines.

from pyspark import SparkContext

sc = SparkContext("local", "TextFileExample")
rdd = sc.textFile(r"file:///C:\Users\Sandeep\Desktop\data\wordcount.txt")
print(rdd.collect())
