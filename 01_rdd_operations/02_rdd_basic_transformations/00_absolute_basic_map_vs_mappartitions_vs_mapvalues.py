from pyspark import SparkContext

sc = SparkContext("local", "RDD Transformation Exercises")

# -------------------- MAP Exercises --------------------

# Exercise 1: Square Numbers
rdd1 = sc.parallelize([1, 2, 3, 4])
print("Map Ex1:", rdd1.map(lambda x: x ** 2).collect())

# Exercise 2: Add 10 to Each Number
rdd2 = sc.parallelize([5, 10, 15])
print("Map Ex2:", rdd2.map(lambda x: x + 10).collect())

# Exercise 3: Convert Strings to Uppercase
rdd3 = sc.parallelize(["spark", "map", "python"])
print("Map Ex3:", rdd3.map(lambda x: x.upper()).collect())

# Exercise 4: Get Length of Each Word
rdd4 = sc.parallelize(["apple", "banana", "fig"])
print("Map Ex4:", rdd4.map(lambda x: len(x)).collect())

# Exercise 5: Add Prefix to Each String
rdd5 = sc.parallelize(["code", "data", "test"])
print("Map Ex5:", rdd5.map(lambda x: "prefix_" + x).collect())

# -------------------- MAPPARTITIONS Exercises --------------------

# Exercise 1: Sum of Each Partition
rdd6 = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
print("MapPartitions Ex1:", rdd6.mapPartitions(lambda it: [sum(it)]).collect())

# Exercise 2: Multiply Each Number by 2 in Each Partition
rdd7 = sc.parallelize([2, 4, 6, 8], 2)
print("MapPartitions Ex2:", rdd7.mapPartitions(lambda it: [x * 2 for x in it]).collect())

# Exercise 3: Convert Strings to Uppercase in Each Partition
rdd8 = sc.parallelize(["cat", "dog", "fish"], 2)
print("MapPartitions Ex3:", rdd8.mapPartitions(lambda it: [word.upper() for word in it]).collect())

# Exercise 4: Count Elements in Each Partition
rdd9 = sc.parallelize(range(10), 4)
print("MapPartitions Ex4:", rdd9.mapPartitions(lambda it: [len(list(it))]).collect())

# Exercise 5: Append Partition Sum to Each Element (Note: not practical this way, just demo)
rdd10 = sc.parallelize([1, 2, 3, 4], 2)
print("MapPartitions Ex5:", rdd10.mapPartitions(lambda it: [(x, sum([x])) for x in it]).collect())

# -------------------- MAPVALUES Exercises --------------------

# Exercise 1: Double the Values
rdd11 = sc.parallelize([("a", 1), ("b", 2)])
print("MapValues Ex1:", rdd11.mapValues(lambda v: v * 2).collect())

# Exercise 2: Convert Values to Uppercase
rdd12 = sc.parallelize([("x", "hello"), ("y", "world")])
print("MapValues Ex2:", rdd12.mapValues(lambda v: v.upper()).collect())

# Exercise 3: Append Length to String Values
rdd13 = sc.parallelize([("id1", "apple"), ("id2", "orange")])
print("MapValues Ex3:", rdd13.mapValues(lambda v: (v, len(v))).collect())

# Exercise 4: Create List from Integer Value
rdd14 = sc.parallelize([("one", 2), ("two", 3)])
print("MapValues Ex4:", rdd14.mapValues(lambda v: list(range(v))).collect())

# Exercise 5: Reverse String Values
rdd15 = sc.parallelize([("item1", "spark"), ("item2", "rdd")])
print("MapValues Ex5:", rdd15.mapValues(lambda v: v[::-1]).collect())


################################## MapPartitionsWithIndex ####################################

# ------------------ Exercise 1 ------------------
# Tag each element with its partition index
rdd1 = sc.parallelize(["apple", "banana", "cherry", "date"], 2)
result1 = rdd1.mapPartitionsWithIndex(lambda idx, it: [(idx, x) for x in it]).collect()
print("Exercise 1 - Tag with partition index:", result1)
# Output: [(0, 'apple'), (0, 'banana'), (1, 'cherry'), (1, 'date')]

# ------------------ Exercise 2 ------------------
# Count number of elements in each partition
rdd2 = sc.parallelize(range(10), 3)
result2 = rdd2.mapPartitionsWithIndex(lambda idx, it: [(idx, len(list(it)))])
print("Exercise 2 - Count elements per partition:", result2)
# Output: [(0, 4), (1, 3), (2, 3)]

# ------------------ Exercise 3 ------------------
# Add partition index to each element (if numeric)
rdd3 = sc.parallelize([10, 20, 30, 40], 2)
result3 = rdd3.mapPartitionsWithIndex(lambda idx, it: [x + idx for x in it]).collect()
print("Exercise 3 - Add index to element:", result3)
# Output: [10, 21, 31, 41]

# ------------------ Exercise 4 ------------------
# Return only the first element of each partition
def first_element_only(idx, it):
    it = list(it)
    return [(idx, it[0])] if it else []

rdd4 = sc.parallelize(["x", "y", "z", "w"], 2)
result4 = rdd4.mapPartitionsWithIndex(first_element_only).collect()
print("Exercise 4 - First element per partition:", result4)
# Output: [(0, 'x'), (1, 'z')]

# ------------------ Exercise 5 ------------------
# Label each element as 'even' or 'odd' based on partition index
def label_even_odd(idx, it):
    label = "even" if idx % 2 == 0 else "odd"
    return [(x, label) for x in it]

rdd5 = sc.parallelize(["a", "b", "c", "d", "e"], 3)
result5 = rdd5.mapPartitionsWithIndex(label_even_odd).collect()
print("Exercise 5 - Label even/odd partition:", result5)
# Output: [('a', 'even'), ('b', 'even'), ('c', 'odd'), ('d', 'odd'), ('e', 'even')]


# -------------------- GLOM Exercises --------------------

# Exercise 1: View Partitions
rdd16 = sc.parallelize([1, 2, 3, 4, 5, 6], 2)
print("Glom Ex1:", rdd16.glom().collect())

# Exercise 2: Count Items per Partition
rdd17 = sc.parallelize([10, 20, 30, 40], 2)
print("Glom Ex2:", rdd17.glom().map(lambda x: len(x)).collect())

# Exercise 3: Max of Each Partition
rdd18 = sc.parallelize([3, 6, 1, 8], 2)
print("Glom Ex3:", rdd18.glom().map(lambda x: max(x)).collect())

# Exercise 4: Concatenate String Elements in Each Partition
rdd19 = sc.parallelize(["a", "b", "c", "d"], 2)
print("Glom Ex4:", rdd19.glom().map(lambda x: "".join(x)).collect())

# Exercise 5: Sort Each Partition
rdd20 = sc.parallelize([4, 3, 1, 2], 2)
print("Glom Ex5:", rdd20.glom().map(sorted).collect())

sc.stop()
