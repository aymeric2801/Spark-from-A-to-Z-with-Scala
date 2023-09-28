# pyspark_intro_demo.py

# Importing required libraries
from pyspark.sql import SparkSession

# Initializing a PySpark session
spark = SparkSession.builder \
    .appName("PySpark Intro") \
    .getOrCreate()

# Creating a DataFrame from a Python list
data = [("Alice", 28), ("Bob", 34), ("Charlie", 45)]
df = spark.createDataFrame(data, ["name", "age"])

# Displaying the DataFrame
df.show()

# Registering the DataFrame as a temporary SQL view
df.createOrReplaceTempView("people")

# Running an SQL query to fetch people older than 30
results = spark.sql("SELECT * FROM people WHERE age > 30")
results.show()

# Optional: Closing the PySpark session
# spark.stop()
