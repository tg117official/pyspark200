# We'll create a scenario where we have a dataset of transactions, and we want to enrich
# these transactions with user information. We'll use a broadcast variable for the user
# information since it's a small dataset that can be reused across multiple transactions efficiently.

from pyspark import SparkContext

def main():
    # Initialize Spark Context
    sc = SparkContext("local", "Broadcast Variables with RDDs")

    # Load data into RDDs
    transactions_rdd = sc.textFile("data/transactions").map(lambda line: line.split(","))
    users_rdd = sc.textFile("data/users").map(lambda line: line.split(","))

    # Skip headers (you might want to handle this more robustly in a real application)
    header1 = transactions_rdd.first()
    transactions_rdd = transactions_rdd.filter(lambda line: line != header1)

    header2 = users_rdd.first()
    users_rdd = users_rdd.filter(lambda line: line != header2)

    # Collect user data into a dictionary and broadcast
    users_dict = users_rdd.collectAsMap()
    print(users_dict)
    broadcast_users = sc.broadcast(users_dict)

    # Map user names to transactions using the broadcast variable
    def add_user_name(transaction):
        user_id = transaction[1]
        user_name = broadcast_users.value.get(user_id, "Unknown")
        return transaction + [user_name]

    enriched_transactions = transactions_rdd.map(add_user_name)

    # Collect and print results
    for transaction in enriched_transactions.collect():
        print(transaction)

    exit = input('Press Enter to exit : ')

    broadcast_users.unpersist()
    # Stop the Spark context

    sc.stop()

if __name__ == "__main__":
    main()
