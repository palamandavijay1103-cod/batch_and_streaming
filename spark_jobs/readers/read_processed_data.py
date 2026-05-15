from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("View Processed Data") \
    .getOrCreate()

df = spark.read.parquet("data/curated/nyc_taxi_gold/")

print("\n=== Schema ===")
df.printSchema()

print("\n=== Sample Data ===")
df.show(10, truncate=False)

print(f"\nTotal Rows: {df.count()}")

spark.stop()