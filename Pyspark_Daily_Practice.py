# Databricks notebook source
# Assuming you have already set up your Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Table ") \
    .getOrCreate()

# Replace 'your_database' and 'your_table' with your actual database and table names
database_name = "default"
table_name = "data_forbes"

# Read the table
df = spark.table(f"{database_name}.{table_name}")

df_tr = df.select('company','marketvalue','sales','profits',(col("sales") - col("profits")).alias('cost'))

df_tr.select('*').where(df_tr.profits > 20).show()

wndw = Window.orderBy(col("Cost").desc())
df_tr = df_tr.withColumn('cost_rank',rank().over(wndw))

# COMMAND ----------

top_high_cost = df_tr.filter(df_tr.cost_rank <= 10).orderBy(df_tr.cost_rank.asc())
top_high_cost.show()

# COMMAND ----------

top_high_cost.write.csv('high_cost_operating_companies.csv')
