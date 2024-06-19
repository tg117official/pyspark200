from pyspark import SparkContext

sc = SparkContext("local", "TextFileExample")

# Read a text file using TextInputFormat
rdd = sc.hadoopFile("data/movies.csv",
                    "org.apache.hadoop.mapred.TextInputFormat",
                    "org.apache.hadoop.io.LongWritable",
                    "org.apache.hadoop.io.Text")

print(rdd.collect())
