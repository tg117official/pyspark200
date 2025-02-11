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


### MAP exercises

# Exercise 1: Square each number
# Given an RDD of integers, square each number.
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
result1 = rdd1.map(lambda x: x ** 2).collect()
print("Exercise 1:", result1)

# Exercise 2: Convert temperature from Celsius to Fahrenheit
# Given an RDD of temperatures in Celsius, convert them to Fahrenheit using the formula F = C * 9/5 + 32.
rdd2 = sc.parallelize([0, 10, 20, 30, 40])
result2 = rdd2.map(lambda c: c * 9/5 + 32).collect()
print("Exercise 2:", result2)

# Exercise 3: Extract first character of each word
# Given an RDD of words, extract the first character of each word.
rdd3 = sc.parallelize(["apple", "banana", "cherry", "date"])
result3 = rdd3.map(lambda word: word[0]).collect()
print("Exercise 3:", result3)

# Exercise 4: Calculate word lengths
# Given an RDD of words, return an RDD of tuples with the word and its length.
rdd4 = sc.parallelize(["spark", "hadoop", "python", "bigdata"])
result4 = rdd4.map(lambda word: (word, len(word))).collect()
print("Exercise 4:", result4)

# Exercise 5: Multiply each number by 10
# Given an RDD of numbers, multiply each number by 10.
rdd5 = sc.parallelize([3, 7, 9, 2, 5])
result5 = rdd5.map(lambda x: x * 10).collect()
print("Exercise 5:", result5)

# Exercise 6: Append domain to usernames
# Given an RDD of usernames, append "@example.com" to each username.
rdd6 = sc.parallelize(["john", "alice", "bob", "charlie"])
result6 = rdd6.map(lambda username: username + "@example.com").collect()
print("Exercise 6:", result6)

# Exercise 7: Convert strings to uppercase
# Given an RDD of strings, convert each string to uppercase.
rdd7 = sc.parallelize(["hello", "world", "spark", "pyspark"])
result7 = rdd7.map(lambda word: word.upper()).collect()
print("Exercise 7:", result7)

# Exercise 8: Extract domain from email addresses
# Given an RDD of email addresses, extract only the domain part.
rdd8 = sc.parallelize(["john@example.com", "alice@gmail.com", "bob@yahoo.com"])
result8 = rdd8.map(lambda email: email.split("@")[1]).collect()
print("Exercise 8:", result8)

# Exercise 9: Convert a list of tuples to key-value pairs
# Given an RDD of tuples (name, age), transform it into an RDD of key-value pairs.
rdd9 = sc.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35)])
result9 = rdd9.map(lambda x: x).collect()  # Identity map for formatting
print("Exercise 9:", result9)

# Exercise 10: Calculate square root of numbers
# Given an RDD of numbers, return an RDD containing the square root of each number.
import math
rdd10 = sc.parallelize([4, 9, 16, 25, 36])
result10 = rdd10.map(lambda x: math.sqrt(x)).collect()
print("Exercise 10:", result10)


### FlatMap exercises 

# Exercise 1: Split words from sentences
# Given an RDD of sentences, flatten the sentences into individual words.
rdd1 = sc.parallelize(["Hello World", "PySpark is fun", "FlatMap transformation"])
result1 = rdd1.flatMap(lambda sentence: sentence.split()).collect()
print("Exercise 1:", result1)

# Exercise 2: Extract all characters from words
# Given an RDD of words, flatten each word into individual characters.
rdd2 = sc.parallelize(["Spark", "Hadoop", "BigData"])
result2 = rdd2.flatMap(lambda word: list(word)).collect()
print("Exercise 2:", result2)

# Exercise 3: Expand numbers into their factors
# Given an RDD of numbers, return an RDD of their factors.
def factors(n):
    return [i for i in range(1, n + 1) if n % i == 0]

rdd3 = sc.parallelize([6, 12, 15])
result3 = rdd3.flatMap(factors).collect()
print("Exercise 3:", result3)

# Exercise 4: Expand tuples into elements
# Given an RDD of tuples, flatten them into individual elements.
rdd4 = sc.parallelize([(1, 2), (3, 4), (5, 6)])
result4 = rdd4.flatMap(lambda x: x).collect()
print("Exercise 4:", result4)

# Exercise 5: Flatten a list of lists
# Given an RDD of lists, return a single flattened RDD.
rdd5 = sc.parallelize([[1, 2, 3], [4, 5], [6, 7, 8, 9]])
result5 = rdd5.flatMap(lambda x: x).collect()
print("Exercise 5:", result5)

# Exercise 6: Generate range from each number
# Given an RDD of numbers, create a range from 1 to that number.
rdd6 = sc.parallelize([3, 5, 2])
result6 = rdd6.flatMap(lambda x: range(1, x+1)).collect()
print("Exercise 6:", result6)

# Exercise 7: Split comma-separated strings into words
# Given an RDD of comma-separated strings, split them into individual words.
rdd7 = sc.parallelize(["apple,banana", "grape,orange", "mango,pear"])
result7 = rdd7.flatMap(lambda x: x.split(",")).collect()
print("Exercise 7:", result7)

# Exercise 8: Extract words from paragraphs
# Given an RDD of paragraphs, split each paragraph into individual words.
rdd8 = sc.parallelize(["This is sentence one. This is two.", "Another sentence here."])
result8 = rdd8.flatMap(lambda x: x.replace(".", "").split()).collect()
print("Exercise 8:", result8)

# Exercise 9: Repeat elements based on value
# Given an RDD of numbers, duplicate each number that many times.
rdd9 = sc.parallelize([1, 2, 3])
result9 = rdd9.flatMap(lambda x: [x] * x).collect()
print("Exercise 9:", result9)

# Exercise 10: Extract digits from numbers
# Given an RDD of integers, split them into their individual digits.
rdd10 = sc.parallelize([123, 456, 789])
result10 = rdd10.flatMap(lambda x: list(str(x))).collect()
print("Exercise 10:", result10)


### filter


# Exercise 1: Filter even numbers
# Given an RDD of numbers, filter out only even numbers.
rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
result1 = rdd1.filter(lambda x: x % 2 == 0).collect()
print("Exercise 1:", result1)

# Exercise 2: Filter words with more than 5 characters
# Given an RDD of words, filter out words that have more than 5 characters.
rdd2 = sc.parallelize(["apple", "banana", "cat", "elephant", "dog", "mango"])
result2 = rdd2.filter(lambda word: len(word) > 5).collect()
print("Exercise 2:", result2)

# Exercise 3: Filter positive numbers
# Given an RDD of numbers, filter only positive numbers.
rdd3 = sc.parallelize([-5, -3, 0, 2, 4, -1, 7, -8])
result3 = rdd3.filter(lambda x: x > 0).collect()
print("Exercise 3:", result3)

# Exercise 4: Filter names starting with 'A'
# Given an RDD of names, filter names that start with 'A'.
rdd4 = sc.parallelize(["Alice", "Bob", "Adam", "Charlie", "Anna"])
result4 = rdd4.filter(lambda name: name.startswith("A")).collect()
print("Exercise 4:", result4)

# Exercise 5: Filter numbers greater than 50
# Given an RDD of numbers, filter out numbers greater than 50.
rdd5 = sc.parallelize([10, 45, 50, 55, 60, 25, 5])
result5 = rdd5.filter(lambda x: x > 50).collect()
print("Exercise 5:", result5)

# Exercise 6: Filter email addresses containing 'gmail'
# Given an RDD of email addresses, filter only those that contain 'gmail'.
rdd6 = sc.parallelize(["john@gmail.com", "alice@yahoo.com", "bob@gmail.com", "charlie@outlook.com"])
result6 = rdd6.filter(lambda email: "gmail" in email).collect()
print("Exercise 6:", result6)

# Exercise 7: Filter words that contain the letter 'e'
# Given an RDD of words, filter only those that contain the letter 'e'.
rdd7 = sc.parallelize(["spark", "hadoop", "bigdata", "data", "machine"])
result7 = rdd7.filter(lambda word: "e" in word).collect()
print("Exercise 7:", result7)

# Exercise 8: Filter odd numbers
# Given an RDD of numbers, filter only odd numbers.
rdd8 = sc.parallelize([10, 15, 22, 33, 44, 55, 66, 77])
result8 = rdd8.filter(lambda x: x % 2 != 0).collect()
print("Exercise 8:", result8)

# Exercise 9: Filter tuples with age greater than 30
# Given an RDD of tuples (name, age), filter out tuples where age is greater than 30.
rdd9 = sc.parallelize([("Alice", 25), ("Bob", 35), ("Charlie", 40), ("David", 28)])
result9 = rdd9.filter(lambda person: person[1] > 30).collect()
print("Exercise 9:", result9)

# Exercise 10: Filter out empty strings
# Given an RDD of strings, filter out empty strings.
rdd10 = sc.parallelize(["hello", "", "world", " ", "pyspark", ""])
result10 = rdd10.filter(lambda x: x.strip() != "").collect()
print("Exercise 10:", result10)



# ------------------------- UNION EXERCISES -------------------------

# Exercise 1: Union of two number RDDs
# Given two RDDs of numbers, return their union.
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize([4, 5, 6, 7, 8])
result1 = rdd1.union(rdd2).collect()
print("Union Exercise 1:", result1)

# Exercise 2: Union of two word RDDs
# Given two RDDs of words, return their union.
rdd3 = sc.parallelize(["apple", "banana"])
rdd4 = sc.parallelize(["grape", "banana", "mango"])
result2 = rdd3.union(rdd4).collect()
print("Union Exercise 2:", result2)

# Exercise 3: Union of two sets of email addresses
# Given two RDDs of email addresses, return their union.
rdd5 = sc.parallelize(["john@gmail.com", "alice@yahoo.com"])
rdd6 = sc.parallelize(["bob@gmail.com", "charlie@outlook.com"])
result3 = rdd5.union(rdd6).collect()
print("Union Exercise 3:", result3)

# Exercise 4: Union of numbers with duplicates
# Given two RDDs, return their union including duplicates.
rdd7 = sc.parallelize([10, 20, 30, 40])
rdd8 = sc.parallelize([30, 40, 50, 60])
result4 = rdd7.union(rdd8).collect()
print("Union Exercise 4:", result4)

# Exercise 5: Union of RDDs with tuples
# Given two RDDs of tuples, return their union.
rdd9 = sc.parallelize([("Alice", 25), ("Bob", 30)])
rdd10 = sc.parallelize([("Charlie", 35), ("Bob", 30)])
result5 = rdd9.union(rdd10).collect()
print("Union Exercise 5:", result5)

# ------------------------- INTERSECTION EXERCISES -------------------------

# Exercise 6: Intersection of two number RDDs
# Given two RDDs of numbers, return their intersection.
rdd11 = sc.parallelize([1, 2, 3, 4, 5])
rdd12 = sc.parallelize([4, 5, 6, 7, 8])
result6 = rdd11.intersection(rdd12).collect()
print("Intersection Exercise 6:", result6)

# Exercise 7: Intersection of two word RDDs
# Given two RDDs of words, return their intersection.
rdd13 = sc.parallelize(["apple", "banana", "cherry"])
rdd14 = sc.parallelize(["banana", "cherry", "date"])
result7 = rdd13.intersection(rdd14).collect()
print("Intersection Exercise 7:", result7)

# Exercise 8: Intersection of two email RDDs
# Given two RDDs of email addresses, return their intersection.
rdd15 = sc.parallelize(["john@gmail.com", "alice@yahoo.com", "bob@gmail.com"])
rdd16 = sc.parallelize(["bob@gmail.com", "charlie@outlook.com", "alice@yahoo.com"])
result8 = rdd15.intersection(rdd16).collect()
print("Intersection Exercise 8:", result8)

# Exercise 9: Intersection of two number sets with duplicates
# Given two RDDs with duplicate numbers, return their intersection (no duplicates in the output).
rdd17 = sc.parallelize([10, 20, 30, 40, 40, 50])
rdd18 = sc.parallelize([30, 40, 50, 60, 70, 40])
result9 = rdd17.intersection(rdd18).collect()
print("Intersection Exercise 9:", result9)

# Exercise 10: Intersection of RDDs with tuples
# Given two RDDs of tuples, return their intersection.
rdd19 = sc.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35)])
rdd20 = sc.parallelize([("Charlie", 35), ("Bob", 30), ("David", 40)])
result10 = rdd19.intersection(rdd20).collect()
print("Intersection Exercise 10:", result10)



### GroupByKey and mapValues

# Exercise 1: Group student scores by subject
# Given an RDD of (subject, score) tuples, group scores by subject.
rdd1 = sc.parallelize([("Math", 85), ("Science", 90), ("Math", 78), ("Science", 88), ("English", 75)])
result1 = rdd1.groupByKey().mapValues(list).collect()
print("Exercise 1:", result1)

# Exercise 2: Group employee salaries by department
# Given an RDD of (department, salary) tuples, group salaries by department.
rdd2 = sc.parallelize([("HR", 50000), ("IT", 70000), ("HR", 52000), ("IT", 75000), ("Finance", 60000)])
result2 = rdd2.groupByKey().mapValues(list).collect()
print("Exercise 2:", result2)

# Exercise 3: Group words by their first letter
# Given an RDD of (first letter, word) tuples, group words by their starting letter.
rdd3 = sc.parallelize([("A", "Apple"), ("B", "Banana"), ("A", "Avocado"), ("B", "Blueberry"), ("C", "Cherry")])
result3 = rdd3.groupByKey().mapValues(list).collect()
print("Exercise 3:", result3)

# Exercise 4: Group orders by customer
# Given an RDD of (customer_id, order_id) tuples, group orders by customer.
rdd4 = sc.parallelize([("C1", "O101"), ("C2", "O102"), ("C1", "O103"), ("C2", "O104"), ("C3", "O105")])
result4 = rdd4.groupByKey().mapValues(list).collect()
print("Exercise 4:", result4)

# Exercise 5: Group employees by city
# Given an RDD of (city, employee_name) tuples, group employees by city.
rdd5 = sc.parallelize([("NY", "John"), ("LA", "Alice"), ("NY", "Bob"), ("LA", "Charlie"), ("SF", "David")])
result5 = rdd5.groupByKey().mapValues(list).collect()
print("Exercise 5:", result5)

# Exercise 6: Group product ratings by product ID
# Given an RDD of (product_id, rating) tuples, group ratings by product.
rdd6 = sc.parallelize([("P1", 4), ("P2", 5), ("P1", 3), ("P2", 4), ("P3", 5)])
result6 = rdd6.groupByKey().mapValues(list).collect()
print("Exercise 6:", result6)

# Exercise 7: Group purchases by customer ID
# Given an RDD of (customer_id, amount) tuples, group purchase amounts by customer.
rdd7 = sc.parallelize([("C1", 200), ("C2", 300), ("C1", 150), ("C2", 400), ("C3", 250)])
result7 = rdd7.groupByKey().mapValues(list).collect()
print("Exercise 7:", result7)

# Exercise 8: Group messages by sender
# Given an RDD of (sender, message) tuples, group messages by sender.
rdd8 = sc.parallelize([("Alice", "Hi"), ("Bob", "Hello"), ("Alice", "How are you?"), ("Bob", "Good morning"), ("Charlie", "Hey")])
result8 = rdd8.groupByKey().mapValues(list).collect()
print("Exercise 8:", result8)

# Exercise 9: Group books by genre
# Given an RDD of (genre, book_title) tuples, group books by genre.
rdd9 = sc.parallelize([("Fiction", "Harry Potter"), ("Non-Fiction", "Sapiens"), ("Fiction", "Lord of the Rings"), ("Non-Fiction", "Educated"), ("Science", "Astrophysics for People in a Hurry")])
result9 = rdd9.groupByKey().mapValues(list).collect()
print("Exercise 9:", result9)

# Exercise 10: Group movie reviews by movie title
# Given an RDD of (movie, review) tuples, group reviews by movie title.
rdd10 = sc.parallelize([("Inception", "Great"), ("Titanic", "Emotional"), ("Inception", "Mind-blowing"), ("Titanic", "Classic"), ("Avatar", "Spectacular")])
result10 = rdd10.groupByKey().mapValues(list).collect()
print("Exercise 10:", result10)



### GroupBy and aggregate 


# Exercise 1: Sum of student scores by subject
# Given an RDD of (subject, score) tuples, calculate the sum of scores for each subject.
rdd1 = sc.parallelize([("Math", 85), ("Science", 90), ("Math", 78), ("Science", 88), ("English", 75)])
result1 = rdd1.groupByKey().mapValues(lambda scores: sum(scores)).collect()
print("Exercise 1:", result1)

# Exercise 2: Average salary by department
# Given an RDD of (department, salary) tuples, calculate the average salary for each department.
rdd2 = sc.parallelize([("HR", 50000), ("IT", 70000), ("HR", 52000), ("IT", 75000), ("Finance", 60000)])
result2 = rdd2.groupByKey().mapValues(lambda salaries: sum(salaries) / len(salaries)).collect()
print("Exercise 2:", result2)

# Exercise 3: Count the number of words by first letter
# Given an RDD of (first letter, word) tuples, count the number of words for each letter.
rdd3 = sc.parallelize([("A", "Apple"), ("B", "Banana"), ("A", "Avocado"), ("B", "Blueberry"), ("C", "Cherry")])
result3 = rdd3.groupByKey().mapValues(lambda words: len(list(words))).collect()
print("Exercise 3:", result3)

# Exercise 4: Find the maximum order amount per customer
# Given an RDD of (customer_id, order_amount) tuples, find the maximum order amount per customer.
rdd4 = sc.parallelize([("C1", 200), ("C2", 300), ("C1", 150), ("C2", 400), ("C3", 250)])
result4 = rdd4.groupByKey().mapValues(lambda amounts: max(amounts)).collect()
print("Exercise 4:", result4)

# Exercise 5: Find the minimum price of products by category
# Given an RDD of (category, price) tuples, find the minimum price in each category.
rdd5 = sc.parallelize([("Electronics", 1200), ("Furniture", 800), ("Electronics", 900), ("Furniture", 750), ("Clothing", 50)])
result5 = rdd5.groupByKey().mapValues(lambda prices: min(prices)).collect()
print("Exercise 5:", result5)

# Exercise 6: Total product sales per product ID
# Given an RDD of (product_id, quantity_sold) tuples, compute the total sales per product.
rdd6 = sc.parallelize([("P1", 5), ("P2", 3), ("P1", 2), ("P2", 4), ("P3", 7)])
result6 = rdd6.groupByKey().mapValues(lambda quantities: sum(quantities)).collect()
print("Exercise 6:", result6)

# Exercise 7: Average product rating per product ID
# Given an RDD of (product_id, rating) tuples, compute the average rating per product.
rdd7 = sc.parallelize([("P1", 4), ("P2", 5), ("P1", 3), ("P2", 4), ("P3", 5)])
result7 = rdd7.groupByKey().mapValues(lambda ratings: sum(ratings) / len(ratings)).collect()
print("Exercise 7:", result7)

# Exercise 8: Total number of employees per city
# Given an RDD of (city, employee_name) tuples, count the number of employees per city.
rdd8 = sc.parallelize([("NY", "John"), ("LA", "Alice"), ("NY", "Bob"), ("LA", "Charlie"), ("SF", "David")])
result8 = rdd8.groupByKey().mapValues(lambda employees: len(list(employees))).collect()
print("Exercise 8:", result8)

# Exercise 9: Find the longest review for each product
# Given an RDD of (product_id, review_text) tuples, find the longest review per product.
rdd9 = sc.parallelize([("P1", "Great product!"), ("P2", "Not bad"), ("P1", "Amazing performance"), ("P2", "Could be better"), ("P3", "Outstanding quality!")])
result9 = rdd9.groupByKey().mapValues(lambda reviews: max(reviews, key=len)).collect()
print("Exercise 9:", result9)

# Exercise 10: Find the customer who made the highest transaction
# Given an RDD of (customer_id, transaction_amount) tuples, find the highest transaction per customer.
rdd10 = sc.parallelize([("C1", 1200), ("C2", 3000), ("C1", 900), ("C2", 2500), ("C3", 4500)])
result10 = rdd10.groupByKey().mapValues(lambda transactions: max(transactions)).collect()
print("Exercise 10:", result10)


# SortByKey


# Exercise 1: Sort numbers by key in ascending order
# Given an RDD of (key, value) pairs, sort the RDD by key in ascending order.
rdd1 = sc.parallelize([(3, "Three"), (1, "One"), (4, "Four"), (2, "Two")])
result1 = rdd1.sortByKey().collect()
print("Exercise 1:", result1)

# Exercise 2: Sort numbers by key in descending order
# Given an RDD of (key, value) pairs, sort the RDD by key in descending order.
result2 = rdd1.sortByKey(ascending=False).collect()
print("Exercise 2:", result2)

# Exercise 3: Sort student scores by student ID
# Given an RDD of (student_id, score), sort it by student_id in ascending order.
rdd3 = sc.parallelize([(102, 85), (101, 90), (104, 78), (103, 88)])
result3 = rdd3.sortByKey().collect()
print("Exercise 3:", result3)

# Exercise 4: Sort employee salaries by employee ID in descending order
# Given an RDD of (employee_id, salary), sort it by employee_id in descending order.
rdd4 = sc.parallelize([(1003, 70000), (1001, 50000), (1004, 75000), (1002, 60000)])
result4 = rdd4.sortByKey(ascending=False).collect()
print("Exercise 4:", result4)

# Exercise 5: Sort words by their first letter
# Given an RDD of (word, frequency) pairs, sort the words alphabetically.
rdd5 = sc.parallelize([("banana", 3), ("apple", 5), ("cherry", 2), ("date", 4)])
result5 = rdd5.sortByKey().collect()
print("Exercise 5:", result5)

# Exercise 6: Sort orders by order date
# Given an RDD of (order_date, order_id), sort the orders by date.
rdd6 = sc.parallelize([("2024-02-10", "O103"), ("2024-01-15", "O101"), ("2024-03-05", "O105"), ("2024-02-20", "O104")])
result6 = rdd6.sortByKey().collect()
print("Exercise 6:", result6)

# Exercise 7: Sort employees by names
# Given an RDD of (name, age), sort by names alphabetically.
rdd7 = sc.parallelize([("John", 30), ("Alice", 25), ("Bob", 35), ("Charlie", 28)])
result7 = rdd7.sortByKey().collect()
print("Exercise 7:", result7)

# Exercise 8: Sort transactions by transaction amount
# Given an RDD of (transaction_id, amount), sort by transaction_id in ascending order.
rdd8 = sc.parallelize([(5002, 1200), (5001, 3000), (5004, 900), (5003, 2500)])
result8 = rdd8.sortByKey().collect()
print("Exercise 8:", result8)

# Exercise 9: Sort books by their title
# Given an RDD of (book_title, copies_sold), sort books by title in ascending order.
rdd9 = sc.parallelize([("Harry Potter", 50000), ("Lord of the Rings", 75000), ("The Alchemist", 60000), ("1984", 45000)])
result9 = rdd9.sortByKey().collect()
print("Exercise 9:", result9)

# Exercise 10: Sort movie ratings by movie name
# Given an RDD of (movie, rating), sort by movie name.
rdd10 = sc.parallelize([("Inception", 4.8), ("Titanic", 4.5), ("Avatar", 4.7), ("Interstellar", 4.9)])
result10 = rdd10.sortByKey().collect()
print("Exercise 10:", result10)



### Join

# ------------------------- INNER JOIN EXERCISES -------------------------

# Exercise 1: Join student names with their scores
# Given two RDDs (student_id, name) and (student_id, score), perform an inner join.
students = sc.parallelize([(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")])
scores = sc.parallelize([(1, 85), (2, 90), (3, 78)])
result1 = students.join(scores).collect()
print("Exercise 1:", result1)

# Exercise 2: Join employees with their departments
# Given two RDDs (emp_id, emp_name) and (emp_id, department), perform an inner join.
employees = sc.parallelize([(101, "John"), (102, "Alice"), (103, "Bob")])
departments = sc.parallelize([(101, "HR"), (102, "IT"), (104, "Finance")])
result2 = employees.join(departments).collect()
print("Exercise 2:", result2)

# ------------------------- LEFT OUTER JOIN EXERCISES -------------------------

# Exercise 3: Left join student names with scores
# Perform a left join to keep all students even if they don’t have a score.
result3 = students.leftOuterJoin(scores).collect()
print("Exercise 3:", result3)

# Exercise 4: Left join employees with departments
# Perform a left join to keep all employees even if they don’t have a department.
result4 = employees.leftOuterJoin(departments).collect()
print("Exercise 4:", result4)

# ------------------------- RIGHT OUTER JOIN EXERCISES -------------------------

# Exercise 5: Right join students with scores
# Perform a right join to keep all scores even if they don’t have a matching student.
result5 = students.rightOuterJoin(scores).collect()
print("Exercise 5:", result5)

# Exercise 6: Right join employees with departments
# Perform a right join to keep all departments even if they don’t have a matching employee.
result6 = employees.rightOuterJoin(departments).collect()
print("Exercise 6:", result6)

# ------------------------- FULL OUTER JOIN EXERCISES -------------------------

# Exercise 7: Full join students with scores
# Perform a full join to keep all students and scores even if there’s no match.
result7 = students.fullOuterJoin(scores).collect()
print("Exercise 7:", result7)

# Exercise 8: Full join employees with departments
# Perform a full join to keep all employees and departments even if there’s no match.
result8 = employees.fullOuterJoin(departments).collect()
print("Exercise 8:", result8)

# ------------------------- COMPLEX JOIN EXERCISES -------------------------

# Exercise 9: Join customer orders with order amounts
# Given (customer_id, customer_name) and (customer_id, order_amount), perform an inner join.
customers = sc.parallelize([(201, "Eve"), (202, "Frank"), (203, "Grace")])
orders = sc.parallelize([(201, 500), (202, 700), (204, 800)])
result9 = customers.join(orders).collect()
print("Exercise 9:", result9)

# Exercise 10: Left join product names with their prices
# Given (product_id, product_name) and (product_id, price), perform a left join.
products = sc.parallelize([(301, "Laptop"), (302, "Tablet"), (303, "Phone")])
prices = sc.parallelize([(301, 1000), (303, 500)])
result10 = products.leftOuterJoin(prices).collect()
print("Exercise 10:", result10)


### reduceByKey

# Exercise 1: Sum of student scores by subject
# Given an RDD of (subject, score) tuples, calculate the total score per subject.
rdd1 = sc.parallelize([("Math", 85), ("Science", 90), ("Math", 78), ("Science", 88), ("English", 75)])
result1 = rdd1.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 1:", result1)

# Exercise 2: Total salary by department
# Given an RDD of (department, salary) tuples, calculate the total salary per department.
rdd2 = sc.parallelize([("HR", 50000), ("IT", 70000), ("HR", 52000), ("IT", 75000), ("Finance", 60000)])
result2 = rdd2.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 2:", result2)

# Exercise 3: Maximum score per subject
# Given an RDD of (subject, score), find the highest score in each subject.
result3 = rdd1.reduceByKey(lambda a, b: max(a, b)).collect()
print("Exercise 3:", result3)

# Exercise 4: Minimum salary per department
# Given an RDD of (department, salary), find the minimum salary in each department.
result4 = rdd2.reduceByKey(lambda a, b: min(a, b)).collect()
print("Exercise 4:", result4)

# Exercise 5: Count occurrences of words
# Given an RDD of (word, 1) pairs, count the occurrences of each word.
rdd5 = sc.parallelize([("spark", 1), ("hadoop", 1), ("spark", 1), ("hadoop", 1), ("bigdata", 1)])
result5 = rdd5.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 5:", result5)

# Exercise 6: Total quantity sold per product
# Given an RDD of (product, quantity_sold) tuples, compute the total quantity sold per product.
rdd6 = sc.parallelize([("Laptop", 5), ("Tablet", 3), ("Laptop", 2), ("Tablet", 4), ("Phone", 7)])
result6 = rdd6.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 6:", result6)

# Exercise 7: Total purchase amount per customer
# Given an RDD of (customer_id, amount), compute the total amount spent per customer.
rdd7 = sc.parallelize([("C1", 200), ("C2", 300), ("C1", 150), ("C2", 400), ("C3", 250)])
result7 = rdd7.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 7:", result7)

# Exercise 8: Sum of review ratings per product
# Given an RDD of (product_id, rating), compute the sum of all ratings per product.
rdd8 = sc.parallelize([("P1", 4), ("P2", 5), ("P1", 3), ("P2", 4), ("P3", 5)])
result8 = rdd8.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 8:", result8)

# Exercise 9: Total transaction amount per account
# Given an RDD of (account_id, transaction_amount), compute the total transaction amount per account.
rdd9 = sc.parallelize([("A1", 1000), ("A2", 500), ("A1", 200), ("A2", 300), ("A3", 400)])
result9 = rdd9.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 9:", result9)

# Exercise 10: Count of employees per department
# Given an RDD of (department, 1) tuples, count the number of employees per department.
rdd10 = sc.parallelize([("HR", 1), ("IT", 1), ("HR", 1), ("IT", 1), ("Finance", 1)])
result10 = rdd10.reduceByKey(lambda a, b: a + b).collect()
print("Exercise 10:", result10)


# ------------------------- DISTINCT EXERCISE -------------------------
# Exercise 1: Get unique numbers from an RDD
# Given an RDD of numbers, remove duplicates.
rdd1 = sc.parallelize([1, 2, 2, 3, 4, 4, 5])
result1 = rdd1.distinct().collect()
print("Exercise 1 (Distinct):", result1)

# ------------------------- CARTESIAN EXERCISE -------------------------
# Exercise 2: Cartesian product of two sets
# Given two RDDs of numbers, compute the cartesian product.
rdd2_a = sc.parallelize([1, 2, 3])
rdd2_b = sc.parallelize(["A", "B", "C"])
result2 = rdd2_a.cartesian(rdd2_b).collect()
print("Exercise 2 (Cartesian):", result2)

# ------------------------- ZIP EXERCISE -------------------------
# Exercise 3: Zip two RDDs together
# Given two RDDs, zip them to form pairs.
rdd3_a = sc.parallelize(["Alice", "Bob", "Charlie"])
rdd3_b = sc.parallelize([25, 30, 35])
result3 = rdd3_a.zip(rdd3_b).collect()
print("Exercise 3 (Zip):", result3)

# ------------------------- SUBTRACT EXERCISE -------------------------
# Exercise 4: Subtract elements from an RDD
# Given two RDDs, remove elements in the second RDD from the first.
rdd4_a = sc.parallelize([1, 2, 3, 4, 5])
rdd4_b = sc.parallelize([3, 4])
result4 = rdd4_a.subtract(rdd4_b).collect()
print("Exercise 4 (Subtract):", result4)

# ------------------------- COALESCE EXERCISE -------------------------
# Exercise 5: Reduce the number of partitions
# Given an RDD, reduce the number of partitions using coalesce.
rdd5 = sc.parallelize(range(1, 21), numSlices=4)
result5 = rdd5.coalesce(2).glom().collect()
print("Exercise 5 (Coalesce):", result5)

# ------------------------- REPARTITION EXERCISE -------------------------
# Exercise 6: Increase the number of partitions
# Given an RDD, increase the number of partitions using repartition.
result6 = rdd5.repartition(6).glom().collect()
print("Exercise 6 (Repartition):", result6)

# ------------------------- PIPE EXERCISE -------------------------
# Exercise 7: Use pipe to execute a shell command
# Given an RDD of strings, use pipe to execute a shell command (echo).
rdd7 = sc.parallelize(["hello", "world"])
result7 = rdd7.pipe("echo").collect()
print("Exercise 7 (Pipe):", result7)

# ------------------------- KEYBY EXERCISE -------------------------
# Exercise 8: Convert elements into key-value pairs
# Given an RDD of words, convert it into key-value pairs using keyBy.
rdd8 = sc.parallelize(["spark", "hadoop", "bigdata"])
result8 = rdd8.keyBy(lambda word: word[0]).collect()
print("Exercise 8 (KeyBy):", result8)

# ------------------------- MAPPARTITIONS EXERCISE -------------------------
# Exercise 9: Apply a function to each partition
# Given an RDD, use mapPartitions to apply a function to each partition.
def partition_func(iterable):
    return [sum(iterable)]  # Summing all values in a partition

rdd9 = sc.parallelize(range(1, 11), numSlices=2)
result9 = rdd9.mapPartitions(partition_func).collect()
print("Exercise 9 (MapPartitions):", result9)

# ------------------------- GLOM EXERCISE -------------------------
# Exercise 10: View elements within each partition
# Given an RDD, use glom to see partition-wise data.
result10 = rdd9.glom().collect()
print("Exercise 10 (Glom):", result10)



# ------------------------- FLATMAPVALUES EXERCISE -------------------------
# Exercise 1: Expand values in key-value pairs
# Given an RDD of (key, list_of_values), expand the values while keeping the key.
rdd1 = sc.parallelize([("A", [1, 2, 3]), ("B", [4, 5]), ("C", [6, 7, 8, 9])])
result1 = rdd1.flatMapValues(lambda x: x).collect()
print("Exercise 1 (FlatMapValues):", result1)

# ------------------------- COMBINEBYKEY EXERCISE -------------------------
# Exercise 2: Compute the average score per student
# Given an RDD of (student, score), compute the average score per student.
rdd2 = sc.parallelize([("Alice", 85), ("Bob", 90), ("Alice", 78), ("Bob", 88), ("Charlie", 75)])
result2 = rdd2.combineByKey(lambda v: (v, 1),  # Create a tuple (value, count)
                            lambda acc, v: (acc[0] + v, acc[1] + 1),  # Aggregate
                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # Merge
                            ).mapValues(lambda x: x[0] / x[1]).collect()
print("Exercise 2 (CombineByKey - Average Scores):", result2)

# ------------------------- MAPVALUES EXERCISE -------------------------
# Exercise 3: Increase salary of employees by 10%
# Given an RDD of (employee, salary), increase each salary by 10%.
rdd3 = sc.parallelize([("John", 50000), ("Alice", 60000), ("Bob", 55000)])
result3 = rdd3.mapValues(lambda salary: salary * 1.1).collect()
print("Exercise 3 (MapValues):", result3)

# ------------------------- PARTITIONBY EXERCISE -------------------------
# Exercise 4: Partition an RDD based on key hash
# Given an RDD of key-value pairs, partition it into 3 partitions.
rdd4 = sc.parallelize([("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5)])
result4 = rdd4.partitionBy(3).glom().collect()
print("Exercise 4 (PartitionBy):", result4)

# ------------------------- LOOKUP EXERCISE -------------------------
# Exercise 5: Find all values for a given key
# Given an RDD of (key, value) pairs, retrieve all values for a given key.
rdd5 = sc.parallelize([("A", 10), ("B", 20), ("A", 30), ("B", 40), ("C", 50)])
result5 = rdd5.lookup("A")
print("Exercise 5 (Lookup):", result5)

# ------------------------- COUNTBYKEY EXERCISE -------------------------
# Exercise 6: Count occurrences of each key
# Given an RDD of (category, product), count occurrences of each category.
rdd6 = sc.parallelize([("Electronics", "Laptop"), ("Furniture", "Table"), 
                       ("Electronics", "Phone"), ("Furniture", "Chair"), ("Electronics", "Tablet")])
result6 = rdd6.countByKey()
print("Exercise 6 (CountByKey):", result6)

# ------------------------- REDUCE EXERCISE -------------------------
# Exercise 7: Find the sum of all numbers
# Given an RDD of numbers, find the sum using reduce.
rdd7 = sc.parallelize([10, 20, 30, 40, 50])
result7 = rdd7.reduce(lambda a, b: a + b)
print("Exercise 7 (Reduce - Sum of Numbers):", result7)

# ------------------------- AGGREGATE EXERCISE -------------------------
# Exercise 8: Find min, max, and sum of numbers
# Given an RDD of numbers, find min, max, and sum using aggregate.
rdd8 = sc.parallelize([10, 20, 30, 40, 50])
result8 = rdd8.aggregate((float('inf'), float('-inf'), 0),
                         lambda acc, x: (min(acc[0], x), max(acc[1], x), acc[2] + x),
                         lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]), acc1[2] + acc2[2])
                         )
print("Exercise 8 (Aggregate - Min, Max, Sum):", result8)

# ------------------------- GROUPBY EXERCISE -------------------------
# Exercise 9: Group words by their first letter
# Given an RDD of words, group words by their first letter.
rdd9 = sc.parallelize(["apple", "banana", "cherry", "avocado", "blueberry", "carrot"])
result9 = rdd9.groupBy(lambda word: word[0]).mapValues(list).collect()
print("Exercise 9 (GroupBy):", result9)

# ------------------------- FILTERWITHKEY EXERCISE -------------------------
# Exercise 10: Filter key-value pairs where the key starts with 'A'
# Given an RDD of (key, value) pairs, filter those where the key starts with 'A'.
rdd10 = sc.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Anna", 28)])
result10 = rdd10.filter(lambda kv: kv[0].startswith("A")).collect()
print("Exercise 10 (Filter with Key Condition):", result10)

# Stop SparkContext
sc.stop()
