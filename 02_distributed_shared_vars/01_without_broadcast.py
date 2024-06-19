from pyspark import SparkContext

def main():
    # Initialize Spark Context
    sc = SparkContext("local", "Join RDDs Example")

    # Load data into RDDs
    transactions_rdd = sc.textFile("data/transactions").map(lambda line: line.split(","))
    users_rdd = sc.textFile("data/users").map(lambda line: line.split(","))

    # Skip headers (you might want to handle this more robustly in a real application)
    header1 = transactions_rdd.first()
    transactions_rdd = transactions_rdd.filter(lambda line: line != header1)

    header2 = users_rdd.first()
    users_rdd = users_rdd.filter(lambda line: line != header2)

    # Convert users RDD to a pair RDD with user_id as the key
    users_pair_rdd = users_rdd.map(lambda user: (user[0], user[1]))

    # Convert transactions RDD to a pair RDD with user_id as the key
    transactions_pair_rdd = transactions_rdd.map(lambda transaction: (transaction[1], transaction))

    # Join the RDDs on user_id
    joined_rdd = transactions_pair_rdd.join(users_pair_rdd)

    for i in joined_rdd.collect():
        print(i)

    # Map to format the output as desired (flattening the tuple)
    result_rdd = joined_rdd.map(lambda x: (x[1][0][0], x[0], x[1][0][2], x[1][1]))

    # Collect and print results
    for record in result_rdd.collect():
        print(record)

    # Stop the Spark context
    leave = input("Press Enter to End the application : ")
    sc.stop()

if __name__ == "__main__":
    main()
