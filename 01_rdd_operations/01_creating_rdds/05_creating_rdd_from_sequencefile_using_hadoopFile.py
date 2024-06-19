from pyspark import SparkContext

sc = SparkContext("local", "SequenceFileExample")

# Read a sequence file
rdd = sc.hadoopFile("data/movies_sequence",
                    "org.apache.hadoop.mapred.SequenceFileInputFormat",
                    "org.apache.hadoop.io.Text",
                    "org.apache.hadoop.io.IntWritable")

# Convert RDD to display as tuples
# result = rdd.map(lambda x: (x[0].toString(), int(x[1].get())))

print(rdd.collect())
