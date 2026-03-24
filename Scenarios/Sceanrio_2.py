# Databricks notebook source
# MAGIC %md
# MAGIC ### STREAMING QUERY

# COMMAND ----------

#A retail company receives daily sales transaction files from multiple store locations in an Azure Data Lake folder. Instead of reprocessing all historical data every day, the data engineering team uses Spark Structured Streaming to incrementally load only the newly arrived files into a Delta table. This ensures timely updates to analytics dashboards while optimizing compute costs and processing time.

# COMMAND ----------

my_schema = """
  order_id INT,
  customer_id INT,
  order_date DATE,
  amount DOUBLE
"""

# COMMAND ----------

df = spark.readStream.format("csv")\
    .option("header", "true")\
    .schema(my_schema)\
    .load("/Volumes/pyspark_cata/source/db_volume/streamSource/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STREAMING OUTPUT

# COMMAND ----------

df.writeStream.format("delta")\
    .option("checkpointLocation", "/Volumes/pyspark_cata/source/db_volume/streamCheckpoint/")\
    .option("mergeSchema","True")\
    .trigger(once=True)\
    .start("/Volumes/pyspark_cata/source/db_volume/streamSink/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/pyspark_cata/source/db_volume/streamSink/data/`

# COMMAND ----------

