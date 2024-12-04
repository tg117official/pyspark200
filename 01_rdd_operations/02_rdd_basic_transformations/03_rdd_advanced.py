from pyspark import SparkConf, SparkContext
from random import sample

# Initialize SparkContext
conf = SparkConf().setAppName("AdvancedRDDExercises").setMaster("local")
sc = SparkContext(conf=conf)

# Exercise 16: Compute PageRank for a Small Graph
edges = [
    ("A", "B"), ("A", "C"), ("B", "C"),
    ("C", "A"), ("D", "C"), ("D", "E"),
    ("E", "C")
]
edges_rdd = sc.parallelize(edges)
nodes = edges_rdd.flatMap(lambda x: x).distinct()
ranks = nodes.map(lambda x: (x, 1.0))
links = edges_rdd.groupByKey()
for _ in range(10):
    contributions = links.join(ranks).flatMap(
        lambda x: [(dest, x[1][1] / len(x[1][0])) for dest in x[1][0]]
    )
    ranks = contributions.reduceByKey(lambda a, b: a + b).mapValues(lambda x: 0.15 + 0.85 * x)
print("Exercise 16: PageRank:", ranks.collect())

# Exercise 17: Group Top-N Products by Category
sales_data = [
    ("electronics", "Laptop", 1000), ("electronics", "Mobile", 800),
    ("electronics", "Tablet", 500), ("clothing", "Jeans", 200),
    ("clothing", "Shirt", 150), ("clothing", "Jacket", 300)
]
sales_rdd = sc.parallelize(sales_data)
top_products_by_category = (
    sales_rdd.map(lambda x: (x[0], (x[1], x[2])))  # (category, (product, sales))
    .groupByKey()
    .mapValues(lambda products: sorted(products, key=lambda x: -x[1])[:2])  # Top 2
)
print("Exercise 17: Top products by category:", top_products_by_category.collect())

# Exercise 18: Analyze Word Co-Occurrence
text_data = [
    "the quick brown fox",
    "jumps over the lazy dog"
]
text_rdd = sc.parallelize(text_data)
co_occurrences = (
    text_rdd.flatMap(lambda line: line.split())  # Split into words
    .zipWithIndex()  # Add index
    .flatMap(lambda x: [((x[1], x[1] + 1), x[0])])  # Sliding window
)
print("Exercise 18: Word co-occurrences:", co_occurrences.collect())

# Exercise 19: Calculate Median for Large Dataset
large_data = [12, 3, 5, 7, 19, 8, 21, 13]
large_rdd = sc.parallelize(large_data)
sorted_rdd = large_rdd.sortBy(lambda x: x).zipWithIndex().map(lambda x: (x[1], x[0]))
count = large_rdd.count()
if count % 2 == 0:
    median = (
        sorted_rdd.lookup(count // 2 - 1)[0] +
        sorted_rdd.lookup(count // 2)[0]
    ) / 2
else:
    median = sorted_rdd.lookup(count // 2)[0]
print("Exercise 19: Median value:", median)

# Exercise 20: Implement K-Means Clustering
points = [(1, 2), (2, 3), (3, 4), (8, 8), (9, 9), (10, 10), (25, 30), (28, 32), (30, 35)]
points_rdd = sc.parallelize(points)
k = 3
centroids = points_rdd.takeSample(False, k)
for _ in range(10):
    clusters = points_rdd.map(
        lambda point: (min(range(k), key=lambda i: (point[0] - centroids[i][0])**2 + (point[1] - centroids[i][1])**2), point)
    )
    new_centroids = clusters.groupByKey().mapValues(
        lambda points: (
            sum(p[0] for p in points) / len(points),
            sum(p[1] for p in points) / len(points)
        )
    ).collect()
    centroids = [x[1] for x in sorted(new_centroids)]
print("Exercise 20: Final centroids:", centroids)

# Stop SparkContext
sc.stop()
