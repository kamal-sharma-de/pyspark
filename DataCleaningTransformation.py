from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, struct, avg, sum, desc
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, ArrayType, StructField

# Create SparkSession
spark = SparkSession.builder.appName("DataEngineeringDemo").getOrCreate()

# Sample Data (in-memory)
data = [
    ("Alice", 25, 70.0, ["Java", "Python"]),
    ("Bob", 30, 85.5, ["Scala", "SQL"]),
    ("Charlie", 28, 90.2, ["Python", "R"]),
    ("David", 32, 78.1, ["Java", "C++"]),
]

# Create a DataFrame schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("score", DoubleType(), True),
    StructField("skills", ArrayType(StringType()), True),
])

# Create a DataFrame from the data
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Data Transformations

# Explode skills array
df_exploded = df.withColumn("skill", explode("skills"))

# Filter by skill
df_filtered = df_exploded.where(col("skill") == "Python")

# Group by name and calculate average score
df_grouped = df.groupBy("name").agg(avg("score").alias("average_score"))

# Group by skill and calculate total score
df_skill_sum = df_exploded.groupBy("skill").agg(sum("score").alias("total_score")).orderBy(desc("total_score"))

# Display results
print("Original DataFrame:")
df.show()

print("DataFrame with skills exploded:")
df_exploded.show()

print("DataFrame filtered by skill 'Python':")
df_filtered.show()

print("Average score grouped by name:")
df_grouped.show()

print("Total score grouped by skill (descending):")
df_skill_sum.show()

# Stop SparkSession
spark.stop()
