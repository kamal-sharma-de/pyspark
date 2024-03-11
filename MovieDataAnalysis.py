from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Movie Data Analysis").getOrCreate()

# Define path to movie data file (replace with your actual path)
movie_data_path = "path/to/your/movie.csv"  # Replace with your actual path and file format (CSV, Parquet etc.)

# Read movie data as DataFrame
movies = spark.read.format("csv").option("header", True).option("inferSchema", True).load(movie_data_path)

# Filter for movies with a rating above 8
high_rated_movies = movies.filter(movies.rating > 8)

# Group movies by genre and calculate average rating
avg_rating_by_genre = high_rated_movies.groupBy("genre").avg("rating")

# Find the genre with the highest average rating
highest_rated_genre = avg_rating_by_genre.sort("avg(rating)", ascending=False).limit(1).select("genre").first()

# Print results
print(f"Genre with the Highest Average Rating (above 8): {highest_rated_genre.genre}")

# Stop the SparkSession
spark.stop()
