from pyspark.sql import SparkSession

# Create a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Read Hive Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Specify the Hive database and table you want to read
database_name = "hive_db"
table_name = "car_insurance_data"

# Read the Hive table into a DataFrame
df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")

# Show the DataFrame
df.show(5)

# Optionally, you can perform additional operations on the DataFrame
# For example, print the schema
df.printSchema()

# Stop the Spark session
spark.stop()
