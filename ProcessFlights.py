from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("FlightManagement").getOrCreate()

# Load flight data 
flights_df = spark.read.csv("path/to/data/flights.csv", inferSchema=True)

# Example operations:

# 1. Find flights from a specific origin to a specific destination
origin = "SFO"
destination = "JFK"
filtered_flights = flights_df.where( (flights_df.origin == origin) & (flights_df.destination == destination) )
filtered_flights.show()  # Display results

# 2. Calculate average flight duration by origin
avg_duration_by_origin = flights_df.groupBy("origin").avg("duration_minutes")
avg_duration_by_origin.show()  # Display results

# 3. Find the most frequent delay reason (if delay information exists)
if 'delay_reason' in flights_df.columns:
  most_frequent_delay = flights_df.groupBy("delay_reason").count().sort("count", ascending=False).limit(1)
  most_frequent_delay.show()  # Display results
