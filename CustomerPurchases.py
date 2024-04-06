from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, year, month

# Create a SparkSession
spark = SparkSession.builder.appName("CustomerPurchaseAnalysis").getOrCreate()

# Read customer purchase data from CSV file
customer_purchases = spark.read.csv("customer_purchases.csv", inferSchema=True, header=True)

# Handle missing values (consider replacing with appropriate strategy)
customer_purchases = customer_purchases.dropna()  # Drop rows with missing values

# Convert purchase_date to date type
customer_purchases = customer_purchases.withColumn(
    "purchase_date", col("purchase_date").cast("date")
)

# Analysis Tasks

# 1. Total purchases per customer
total_purchases_per_customer = (
    customer_purchases.groupBy("customer_id").agg(count("product_id").alias("total_purchases"))
)

# 2. Top 10 products by revenue
total_revenue_per_product = customer_purchases.groupBy("product_id").agg(
    sum("price").alias("total_revenue")
)
top_10_products = total_revenue_per_product.sort(col("total_revenue").desc()).limit(10)

# Average purchase amount per customer
avg_purchase_per_customer = (
    customer_purchases.groupBy("customer_id")
    .agg(avg("price").alias("average_purchase"))
)

# Purchase trends over time (by year in this example)
purchase_by_year = (
    customer_purchases.groupBy(year("purchase_date").alias("year"))
    .agg(count("product_id").alias("total_purchases"), sum("price").alias("total_revenue"))
)

# Print the results
print("Total Purchases per Customer:")
total_purchases_per_customer.show()

print("\nTop 10 Products by Revenue:")
top_10_products.show()

# Bonus results 
print("\nAverage Purchase Amount per Customer:")
avg_purchase_per_customer.show()

print("\nPurchases by Year:")
purchase_by_year.show()

# Stop the SparkSession
spark.stop()
