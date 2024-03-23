from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, desc, monotonically_increasing_id, rank, split, lit

# Create SparkSession
spark = SparkSession.builder \
   .appName("ELT_ComplexTransformations") \
   .getOrCreate()

# Read data from source (replace with your source details)
df = spark.read.format("jdbc") \
   .option("url", "jdbc:postgresql://postgres_server:5432/database_name") \
   .option("driver", "org.postgresql.Driver") \
   .option("dbtable", "your_source_table") \
   .option("user", "your_username") \
   .option("password", "your_password") \
   .load()

# Define UDF for complex transformation
@udf("double")
def complex_transformation(column1, column2, additional_data):
   transformed_value = column1 * column2 + additional_data  c
   return transformed_value

# Apply transformations
df = df.withColumn("transformed_column", complex_transformation(col("column1"), col("column2"), lit(10))) \
     .withColumn("exploded_array", explode(split(col("array_column"), ","))) \
     .withColumn("rank", rank().over(Window.orderBy(desc("transformed_column")))) \
     .withColumn("id", monotonically_increasing_id())  # For efficient updates
     .withColumn("transformed_column", df.transformed_column.cast("double")) \
     .withColumn("exploded_array", df.exploded_array.cast("int"))  # Optimize data types 

# Write transformed data to target (replace with your target details)
df.write.format("parquet") \
   .mode("append")  # Adjust for overwrite or errorIfExists if needed
   .option("path", "path/to/data/lake/or/warehouse") \
   .save()

spark.stop()
