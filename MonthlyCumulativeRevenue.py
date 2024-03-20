from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("MonthlyCumulativeRevenue").getOrCreate()

# Sample data with varied dates and revenue
data = [
    (100, "2023-10-31"),
    (200, "2023-11-01"),
    (150, "2023-11-15"),
    (300, "2023-12-05"),
    (50, "2024-01-10"),  # Data from the new year
    (75, "2024-01-20"),
    (225, "2024-02-01"),
]

# Create a DataFrame
df = spark.createDataFrame(data, ["revenue", "date"])

# Convert date string to date type
df = df.withColumn("date", df["date"].cast("date"))

# Calculate month-wise cumulative revenue
window_spec = window.orderBy(col('date').cast('month')).partitionBy()  # Optional: partition by year for year-month cumulative sum
df_with_cumulative_revenue = df.withColumn("cumulative_revenue", sum("revenue").over(window_spec))

# Print the result
df_with_cumulative_revenue.show()

# Stop the SparkSession
spark.stop()
