from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SparkWordCount").getOrCreate()

# Read the text file as a DataFrame
text_file_df = spark.read.text("your_text_file.txt")

# Define a function to extract name, speech, and word count
def extract_and_count(line):
    words = line.value.split()
    if len(words) < 2:
        # Handle cases where there are less than 2 words (name + speech)
        return None  # Skip lines with insufficient data
    name = words[0]
    speech = " ".join(words[1:])  # Combine remaining words as speech
    spark_count = speech.lower().count("spark")  # Case-insensitive counting
    return (name, spark_count)

# Apply the extract_and_count function as a transformation
processed_df = text_file_df.rdd.flatMap(extract_and_count).filter(lambda x: x is not None) \
    .toDF(["name", "spark_count"])  # Convert back to DataFrame

# Print the results
processed_df.show()

spark.stop()
