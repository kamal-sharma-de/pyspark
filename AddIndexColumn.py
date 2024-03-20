from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("IndexColumn").getOrCreate()

# Create some sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# Function to add an index-like column
def create_index_column(df):
  return df.withColumn("index", lit(df.spark_partition_id()))

# Apply the function to create the DataFrame with index column
df_with_index = df.transform(create_index_column)

# Print the DataFrame with the new index column
df_with_index.show()

# Stop the SparkSession
spark.stop()
