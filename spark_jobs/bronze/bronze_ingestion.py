from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Batch ETL") \
    .getOrCreate()

# Read parquet data
df = spark.read.parquet("data/raw/nyc_taxi/*.parquet")

# Show schema
print("\n=== Schema ===")
df.printSchema()

# Show sample data
print("\n=== Sample Data ===")
df.show(5)

# Basic cleaning
cleaned_df = df.filter(col("trip_distance") > 0)

# Select important columns
final_df = cleaned_df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount"
)

# Write processed data
final_df.write.mode("overwrite") \
    .parquet("data/processed/nyc_taxi/")

print("\n✅ ETL Job Completed Successfully")

# Stop Spark session
spark.stop()