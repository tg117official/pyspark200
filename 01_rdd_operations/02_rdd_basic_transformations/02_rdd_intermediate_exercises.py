from pyspark import SparkConf, SparkContext

# Initialize SparkContext
conf = SparkConf().setAppName("AdvancedRDDExercises").setMaster("local")
sc = SparkContext(conf=conf)

# Exercise 11: Calculate the average rating per product from a dataset
data11 = [
    ("product1", 4), ("product2", 5), ("product1", 3),
    ("product2", 4), ("product3", 5), ("product1", 2)
]
rdd11 = sc.parallelize(data11)
ratings_per_product = (
    rdd11
    .mapValues(lambda x: (x, 1))  # (product, (rating, count))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # Aggregate ratings and counts
    .mapValues(lambda x: x[0] / x[1])  # Calculate average
)
print("Exercise 11: Average rating per product:", ratings_per_product.collect())

# Exercise 12: Find the top 3 most frequently occurring words in a text
data12 = ["the quick brown fox jumps over the lazy dog", "the fox is quick and the dog is lazy"]
rdd12 = sc.parallelize(data12)
word_counts = (
    rdd12.flatMap(lambda line: line.split())  # Split lines into words
    .map(lambda word: (word, 1))  # Create (word, 1) pairs
    .reduceByKey(lambda a, b: a + b)  # Aggregate counts
    .sortBy(lambda x: -x[1])  # Sort by count in descending order
)
top_3_words = word_counts.take(3)
print("Exercise 12: Top 3 most frequent words:", top_3_words)

# Exercise 13: Find the total sales amount per category
data13 = [
    ("electronics", 200), ("clothing", 100), ("electronics", 300),
    ("clothing", 150), ("groceries", 50), ("groceries", 80)
]
rdd13 = sc.parallelize(data13)
total_sales = (
    rdd13.reduceByKey(lambda a, b: a + b)  # Aggregate sales per category
)
print("Exercise 13: Total sales per category:", total_sales.collect())

# Exercise 14: Perform a join between two datasets
products = [("p1", "Laptop"), ("p2", "Mobile"), ("p3", "Tablet")]
sales = [("p1", 500), ("p2", 300), ("p4", 200)]
rdd_products = sc.parallelize(products)
rdd_sales = sc.parallelize(sales)
joined_data = rdd_products.join(rdd_sales)  # Join on product IDs
print("Exercise 14: Joined data:", joined_data.collect())

# Exercise 15: Find the top-selling product based on sales
data15 = [("product1", 400), ("product2", 600), ("product3", 150), ("product2", 200)]
rdd15 = sc.parallelize(data15)
total_sales_per_product = (
    rdd15.reduceByKey(lambda a, b: a + b)  # Aggregate sales
)
top_selling_product = total_sales_per_product.sortBy(lambda x: -x[1]).take(1)  # Find the top-selling product
print("Exercise 15: Top-selling product:", top_selling_product)


# Stop SparkContext
sc.stop()
