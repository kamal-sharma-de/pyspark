from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FaultToleranceExample").getOrCreate()

# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)

# Apply some transformations
def double(x):
    return x * 2

doubled_rdd = rdd.map(double)

# Now simulate a worker node failure (for illustrative purposes)
failed_executor_id = doubled_rdd.sparkContext.statusTracker().getExecutors()[0]  # Get first executor ID
spark.sparkContext.stopExecutor(failed_executor_id)  # Simulate failure

# Lineage tracking allows recomputation of lost partitions
result = doubled_rdd.collect()
print(result)  # Output: [2, 4, 6, 8, 10] (assuming recovery is successful)
