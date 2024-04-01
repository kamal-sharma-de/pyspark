from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, split, size, desc, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StringType, ArrayType


def complex_transformation(data):
    # This function performs a transformation on a single record
    # Replace this with your specific logic
    # Example: Calculate sentiment score for a sentence
    tokens = split(data["text"], " ")
    sentiment_score = 0  # Implement sentiment analysis logic here
    return (data["id"], tokens, sentiment_score)


# Create SparkSession
spark = SparkSession.builder.appName("Data Pipeline").getOrCreate()

# Define UDF for complex transformation
complex_transformation_udf = udf(complex_transformation, ArrayType(StringType()))

# Read data (replace "path/to/data" with your data source)
data = spark.read.parquet("path/to/data")

# Explode complex column (if applicable)
if "complex_column" in data.columns:
    data = data.withColumn("exploded", explode(col("complex_column")))

# Select relevant columns
data = data.select("id", "text")  # Adjust based on your needs

# Apply complex transformation UDF in a partitioned and sorted manner
data = data.repartition(col("id") % 10)  # Partition by a hash of id for even distribution
data = data.sortWithinPartitions("id")  # Sort within partitions for efficient processing
data = data.withColumn("transformed_data", complex_transformation_udf(col("text")))

# Clean-up and further transformations
data = data.select("id", size(col("transformed_data")).alias("num_tokens"), col("transformed_data")[0].alias("first_token"))
data = data.withColumn("row_id", monotonically_increasing_id())  # Add a unique row identifier for downstream processing

# Write data (replace "path/to/output" with your output location)
data.write.parquet("path/to/output", partitionBy=["id"])

# Stop SparkSession
spark.stop()
