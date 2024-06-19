# Imagine we have a dataset of product sales where each record represents a sale and includes
# a product ID and quantity sold. We want to count how many sales transactions included more
# than 10 items, which might indicate bulk purchases.

from pyspark import SparkContext


def main():
    # Initialize Spark Context
    sc = SparkContext("local", "Accumulator Example")

    # Load data into an RDD from a text file
    sales_rdd = sc.textFile(r"C:\Users\Sandeep\PycharmProjects\pyspark200\02_distributed_shared_vars\data\sales_data").map(lambda line: line.split(","))

    # Skip the header
    header = sales_rdd.first()
    sales_rdd = sales_rdd.filter(lambda line: line != header)

    # Initialize an accumulator
    bulk_sales_count = sc.accumulator(0)

    # Define a function to update the accumulator
    def count_bulk_sales(record):
        product_id, quantity = record
        if int(quantity) > 10:
            bulk_sales_count.add(1)

    # Apply the function to each RDD element
    res = sales_rdd.map(lambda x : count_bulk_sales(x))
    res.collect()

    # Output the result
    print(f"Number of bulk sales transactions (more than 10 items): {bulk_sales_count.value}")

    exit = input("Press Enter to exit Application : ")

    # Stop the Spark context
    sc.stop()


if __name__ == "__main__":
    main()
