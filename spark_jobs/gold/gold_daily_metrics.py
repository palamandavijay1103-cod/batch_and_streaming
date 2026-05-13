from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum,
    avg,
    count
)

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Gold Layer ETL") \
    .getOrCreate()

# Read silver/processed data
df = spark.read.parquet("data/processed/nyc_taxi/")

# Create trip_date column
df = df.withColumn(
    "trip_date",
    to_date(col("tpep_pickup_datetime"))
)

# Gold layer aggregations
gold_df = df.groupBy("trip_date").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare_amount"),
    sum("total_amount").alias("total_revenue")
)

# Show output
print("\n=== GOLD LAYER DATA ===")
gold_df.show(20, truncate=False)

# Write gold layer data
gold_df.write.mode("overwrite") \
    .parquet("data/curated/nyc_taxi_gold/")

print("\n✅ Gold Layer Created Successfully")

spark.stop()