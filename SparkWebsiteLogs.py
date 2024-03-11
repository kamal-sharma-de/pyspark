from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Website Log Analyzer").getOrCreate()

# Define path to log data file (replace with your actual path)
log_data_path = "path/to/your/log.csv"  # Adjust for your file system

# Read log data as DataFrame
website_logs = spark.read.csv(log_data_path, header=True, inferSchema=True)

# Filter for requests with status code 200 (successful requests)
successful_requests = website_logs.filter(website_logs.status == 200)

# Count total number of requests
total_requests = website_logs.count()

# Count total successful requests
successful_requests_count = successful_requests.count()

# Find most frequently requested pages (top 10)
top_pages = successful_requests.groupBy("page").count().orderBy("count", descending=True).limit(10)

# Print results
print(f"Total Website Requests: {total_requests}")
print(f"Successful Requests: {successful_requests_count}")
print("Top 10 Most Requested Pages:")
top_pages.show(truncate=False)  # Show all data without truncation

# Stop the SparkSession
spark.stop()
