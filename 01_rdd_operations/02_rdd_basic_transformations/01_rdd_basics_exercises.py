from pyspark import SparkConf, SparkContext

# Initialize SparkContext
conf = SparkConf().setAppName("BasicRDDExercises").setMaster("local")
sc = SparkContext(conf=conf)

# Exercise 1: Count the number of elements in an RDD
data1 = [1, 2, 3, 4, 5]
rdd1 = sc.parallelize(data1)
print("Exercise 1: Count of elements:", rdd1.count())

# Exercise 2: Find the sum of all elements in an RDD
data2 = [10, 20, 30, 40, 50]
rdd2 = sc.parallelize(data2)
print("Exercise 2: Sum of elements:", rdd2.sum())

# Exercise 3: Filter even numbers from an RDD
data3 = [11, 22, 33, 44, 55, 66]
rdd3 = sc.parallelize(data3)
even_numbers = rdd3.filter(lambda x: x % 2 == 0).collect()
print("Exercise 3: Even numbers:", even_numbers)

# Exercise 4: Create key-value pairs from a list of words
data4 = ["apple", "banana", "apple", "orange", "banana", "apple"]
rdd4 = sc.parallelize(data4)
word_counts = rdd4.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).collect()
print("Exercise 4: Word counts:", word_counts)

# Exercise 5: Find the maximum number in an RDD
data5 = [100, 200, 300, 400, 50]
rdd5 = sc.parallelize(data5)
max_number = rdd5.max()
print("Exercise 5: Maximum number:", max_number)

# Exercise 6: Sort the elements of an RDD in ascending order
data6 = [9, 3, 1, 4, 6, 2]
rdd6 = sc.parallelize(data6)
sorted_rdd = rdd6.sortBy(lambda x: x).collect()
print("Exercise 6: Sorted RDD:", sorted_rdd)

# Exercise 7: Count the occurrences of each number in an RDD
data7 = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
rdd7 = sc.parallelize(data7)
number_counts = rdd7.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).collect()
print("Exercise 7: Number counts:", number_counts)

# Exercise 8: Calculate the average of numbers in an RDD
data8 = [10, 20, 30, 40, 50]
rdd8 = sc.parallelize(data8)
total_sum = rdd8.sum()
count = rdd8.count()
average = total_sum / count
print("Exercise 8: Average of numbers:", average)

# Exercise 9: Find distinct elements in an RDD
data9 = [1, 2, 2, 3, 3, 3, 4, 4, 5]
rdd9 = sc.parallelize(data9)
distinct_elements = rdd9.distinct().collect()
print("Exercise 9: Distinct elements:", distinct_elements)

# Exercise 10: Create a Cartesian product of two RDDs
data10_a = [1, 2, 3]
data10_b = ['a', 'b', 'c']
rdd10_a = sc.parallelize(data10_a)
rdd10_b = sc.parallelize(data10_b)
cartesian_product = rdd10_a.cartesian(rdd10_b).collect()
print("Exercise 10: Cartesian product:", cartesian_product)

# Stop SparkContext
sc.stop()
