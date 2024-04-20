from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cast

# Create SparkSession
spark = SparkSession.builder.appName("ConcatenateDataFrames").getOrCreate()

# Sample DataFrames with different schema
df1 = spark.createDataFrame([("A", 1)], ["name", "age"])
df2 = spark.createDataFrame([("C", 3.14)], ["ID", "value"])

# Rename column in df2 and cast value to int
df2 = df2.withColumnRenamed("ID", "name").withColumn("value", cast("value", "int"))

# Concatenate DataFrames with aligned schema
combined_df = df1.union(df2)

# Show the resulting DataFrame
combined_df.show()

spark.stop()
