from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date
)

# Create Spark session
spark = SparkSession.builder \
    .appName("Silver Layer ETL") \
    .getOrCreate()

# Read bronze/raw data
df = spark.read.parquet("/opt/airflow/data/raw/nyc_taxi/")

print("\n=== Raw Schema ===")
df.printSchema()

# Basic cleaning
cleaned_df = df.filter(
    col("trip_distance") > 0
)

# Add partition column
cleaned_df = cleaned_df.withColumn(
    "trip_date",
    to_date(col("tpep_pickup_datetime"))
)

# Select useful columns
final_df = cleaned_df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "trip_date"
)

print("\n=== Silver Layer Sample Data ===")
final_df.show(5)

# Write partitioned parquet
final_df.write \
    .mode("overwrite") \
    .partitionBy("trip_date") \
    .parquet("/opt/airflow/data/processed/nyc_taxi/")

print("\n✅ Silver Layer ETL Completed")

spark.stop()