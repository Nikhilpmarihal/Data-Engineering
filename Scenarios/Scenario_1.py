# Databricks notebook source
# MAGIC %md
# MAGIC ### QUERYING SOURCE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from pyspark_cata.source.products

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.sql("SELECT * FROM pyspark_cata.source.products")

#DEDUP
df = df.withColumn("dedup",row_number().over(Window.partitionBy("id").orderBy(desc("updatedDate"))))
df = df.filter(col("dedup") == 1).drop("dedup")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERTS

# COMMAND ----------

#A retail company receives daily updates for its product catalog, including new products, price changes, and discontinued items. Instead of overwriting the entire catalog or simply appending new records, they need to upsert the incoming data—updating existing products with the latest information and inserting new products—ensuring the catalog remains accurate and up-to-date in real-time.

# COMMAND ----------

# Creating Delta Object
from delta.tables import DeltaTable

if len(dbutils.fs.ls("/Volumes/pyspark_cata/source/db_volume/products_sink/"))>0:
    dlt_obj = DeltaTable.forPath(
        spark,
        "/Volumes/pyspark_cata/source/db_volume/products_sink/"
    )

    dlt_obj.alias("trg").merge(
        df.alias("src"),
        "src.id = trg.id"
    ).whenMatchedUpdateAll(
        condition="src.updatedDate >= trg.updatedDate"
    ).whenNotMatchedInsertAll() \
     .execute()
    print("This is upserting now")
else:
    df.write.format("delta") \
        .mode("overwrite") \
        .save("/Volumes/pyspark_cata/source/db_volume/products_sink/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/pyspark_cata/source/db_volume/products_sink/`

# COMMAND ----------

