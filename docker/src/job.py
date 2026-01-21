from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SimpleSparkJob") \
    .getOrCreate()

print("\n=== Spark Job Started ===\n")

data = [
    ("Khiem", 25),
    ("Alice", 30),
    ("Bob", 22)
]

df = spark.createDataFrame(data, ["name", "age"])

print("=== Original DataFrame ===")
df.show()

# Transform
df2 = df.withColumn("age_plus_10", df.age + 10)

print("=== Transformed DataFrame ===")
df2.show()

# Đếm số row
count = df2.count()
print(f"=== Total Rows: {count} ===")

# Kết thúc
print("\n=== Spark Job Finished ===")

spark.stop()