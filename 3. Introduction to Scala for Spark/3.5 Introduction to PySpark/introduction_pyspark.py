from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Intro") \
    .config("spark.master", "local[*]") \
    .getOrCreate()


data = [("Alice", 28), ("Bob", 34), ("Charlie", 45)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

df.createOrReplaceTempView("people")
results = spark.sql("SELECT * FROM people WHERE age > 30")
results.show()
