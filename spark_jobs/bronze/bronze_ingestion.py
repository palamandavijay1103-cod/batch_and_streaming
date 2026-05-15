from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Batch ETL") \
    .getOrCreate()

# Read parquet data
df = spark.read.parquet("/opt/airflow/data/raw/nyc_taxi/*.parquet")

# Show schema
print("\n=== Schema ===")
df.printSchema()

# Show sample data
print("\n=== Sample Data ===")
df.show(5)

# Basic cleaning
cleaned_df = df.filter(col("trip_distance") > 0)

# Add trip_date column derived from tpep_pickup_datetime
cleaned_df = cleaned_df.withColumn("trip_date", to_date(col("tpep_pickup_datetime")))

# Select important columns including trip_date
final_df = cleaned_df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "trip_date"
)

# Write processed data
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

output_path = f"/opt/airflow/data/processed/nyc_taxi/{run_id}"

final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .partitionBy("trip_date") \
    .parquet(output_path)




print("\n✅ ETL Job Completed Successfully")

# Stop Spark session
spark.stop()