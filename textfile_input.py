# Import necessary libraries
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Load text file into an RDD
text_rdd = spark.sparkContext.textFile("input.txt")

# Split text into words and map each word to a tuple (word, 1)
word_rdd = text_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))

# Reduce by key to count occurrences of each word
word_count_rdd = word_rdd.reduceByKey(lambda a, b: a + b)

# Sort results in descending order
sorted_word_count_rdd = word_count_rdd.sortBy(lambda x: x[1], False)

# Save results to a text file
sorted_word_count_rdd.saveAsTextFile("output")

# Stop SparkSession
spark.stop()