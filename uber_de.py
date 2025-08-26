from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum, avg, countDistinct, rank, row_number, \
                                   desc, to_date, months_between, current_date
from pyspark.sql.window import Window
from IPython.display import display

# Start Spark Session
spark = SparkSession.builder.appName("PrepExercises").getOrCreate()

# Load dataset
df = spark.read.csv("ncr_ride_bookings.csv", header=True, inferSchema=True)

# 1. Transformation Basics
# Show only completed rides
completed_rides = df.filter(col("Booking Status") == "Completed")

# Create new column Fare_per_km (safe division)
completed_rides = completed_rides.withColumn(
    "Fare_per_km",
    when(col("Ride Distance") > 0, col("Booking Value") / col("Ride Distance")).otherwise(None)
)

# Display first 20 rows (better table view in notebook)
display(completed_rides.select("Booking ID", "Booking Value", "Ride Distance", "Fare_per_km").limit(20))

# Optional: save the filtered data back as a new CSV/Parquet
# completed_rides.write.mode("overwrite").parquet("completed_rides/")