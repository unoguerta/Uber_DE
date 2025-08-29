from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import DoubleType

# Start Spark Session
spark = SparkSession.builder.appName("PrepExercises").getOrCreate()

# Load dataset
df = spark.read.csv("ncr_ride_bookings.csv", header=True, inferSchema=True)

# Count unique customers per Vehicle Type
unique_customers_per_vehicle = df.groupBy("Vehicle Type") \
                                 .agg(countDistinct("Customer ID").alias("Unique_Customers"))

# Convert to Pandas for pretty print
unique_customers_pdf = unique_customers_per_vehicle.toPandas()

print("\n=== Unique Customers per Vehicle Type ===")
print(unique_customers_pdf.to_string(index=False))
