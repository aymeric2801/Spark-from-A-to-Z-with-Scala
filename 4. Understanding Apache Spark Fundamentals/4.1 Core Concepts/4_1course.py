from pyspark import SparkContext

#Initialize a SparkContext
sc = SparkContext("local", "Transformations Example")

#Create an RDD
data_rdd = sc.parallelize([1, 2, 3, 4, 5])

# Use map transformation to square each element 
squared_rdd = data_rdd.map(lambda x: x**2)

# Collect and print the result
print(squared_rdd.collect())

# Use reduce action to sum the elements
total = squared_rdd.reduce(lambda x, y: x + y)

# Print the result
print(total)

# Create a Key-Value pair RDD
data_pair_rdd = sc.parallelize([(1, 2), (3, 4), (3,6)])

# Reduce by key (sum values for the same key)
reduced_rdd = data_pair_rdd.reduceByKey(lambda x, y: x + y)

# Collect and print the result
print(reduced_rdd.collect())

# Create a Key-Value Pair RDD
data_pair_rdd = sc.parallelize([('apple', 1), ('banana', 2), ('apple', 3)])

# Group by key
grouped_rdd = data_pair_rdd.groupByKey()

# Collect and print the result as a dictionary for clarity
print({key: list(values) for key, values in grouped_rdd.collect()})