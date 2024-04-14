from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, field

# Create a SparkSession (assuming you have Spark configured)
spark = SparkSession.builder.appName("ParseComplexJson").getOrCreate()

# Sample complex JSON data (replace with your actual data)
json_data = [
    {
        "id": 1,
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "nested": {
                "level1": "value1",
                "level2": {
                    "level3": [
                        {"key1": "data1", "key2": 10},
                        {"key1": "data2", "key2": 20}
                    ]
                }
            }
        }
    },
]

# Create a DataFrame from the JSON data
df = spark.createDataFrame(json_data)

# Define a recursive function to flatten nested structs
def flatten_struct(col_name, nested_struct):
  if not isinstance(nested_struct, dict):
    return col_name, nested_struct  # Base case: return column name and value
  fields = []
  for key, value in nested_struct.items():
    new_col_name = f"{col_name}.{key}"
    flattened_col, flattened_value = flatten_struct(new_col_name, value)
    fields.append(field(flattened_col, flattened_value))
  return col_name, struct(*fields)  # Return a struct with flattened fields

# Apply the flatten_struct function recursively to all nested structs
flat_cols = []
for col in df.columns:
  flat_col_name, flat_col = flatten_struct(col, df[col])
  flat_cols.append(flat_col.alias(flat_col_name))

# Create a new DataFrame with flattened columns
flat_df = df.select(*flat_cols)

# Apply transformations 
transformed_df = flat_df \
  .withColumn("address_city_uppercase", col("address.city").upper()) \
  .withColumn("nested_level3_key1_length", col("address.nested.level2.level3").apply(lambda x: len(x[0]["key1"])))

# Display the transformed DataFrame
transformed_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
