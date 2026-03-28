# Databricks notebook source
# A retail company maintains a product catalog in its data warehouse. Product details such as name , category and price may chnage over time due to rebranding , category updated or pricing adjustments. To preserve historical data for accurate reporting and trend analysis , the company needs to implement a SDC(Type2) mechanism in pyspark ensuring old records are retained with effective data ranges while new versions are inserted as separate records.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE pyspark_cata.source.customers
# MAGIC (
# MAGIC   id STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   modifiedDate TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO pyspark_cata.source.customers
# MAGIC VALUES
# MAGIC -- ('1', 'arjun.sharma@gmail.com', 'Bengaluru', 'India', current_timestamp()),
# MAGIC -- ('2', 'priya.reddy@yahoo.com', 'Mumbai', 'India', current_timestamp()),
# MAGIC -- ('3', 'rahul.verma@outlook.com', 'Delhi', 'India', current_timestamp()),
# MAGIC -- ('4', 'sneha.iyer@gmail.com', 'Chennai', 'India', current_timestamp()),
# MAGIC -- ('5', 'vikram.nair@yahoo.com', 'Hyderabad', 'India', current_timestamp()),
# MAGIC -- ('6', 'ananya.shetty@outlook.com', 'Pune', 'India', current_timestamp()),
# MAGIC -- ('7', 'rohit.gowda@gmail.com', 'Kolkata', 'India', current_timestamp()),
# MAGIC -- ('8', 'kavya.menon@yahoo.com', 'Ahmedabad', 'India', current_timestamp()),
# MAGIC -- ('9', 'deepak.kumar@outlook.com', 'Jaipur', 'India', current_timestamp()),
# MAGIC -- ('10', 'pooja.patil@gmail.com', 'Bengaluru', 'India', current_timestamp()),
# MAGIC ('11', 'manish.jain@yahoo.com', 'Udupi', 'India', current_timestamp()),
# MAGIC ('9', 'neha.singh@outlook.com', 'Delhi', 'India', current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cata.source.customers

# COMMAND ----------

if spark.catalog.tableExists("pyspark_cata.source.DimCustomers"):

    pass

else:

    spark.sql("""
              CREATE TABLE pyspark_cata.source.DimCustomers
              SELECT *,
                      current_timestamp() as startTime,
                      CAST('3000-01-01' AS TIMESTAMP) as endTime,
                      'Y' as isActive 
              FROM pyspark_cata.source.customers
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cata.source.DimCustomers

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

#SCD (TYPE2)
df = spark.sql("""
               SELECT * FROM pyspark_cata.source.customers
               """)
df = df.withColumn("dedup",row_number().over(Window.partitionBy('id').orderBy(desc('modifiedDate'))))\
    .drop('dedup')

df = df.filter(col('dedup') == 1)

df.createOrReplaceTempView('srctemp')

df = spark.sql("""
              SELECT *,
                      current_timestamp() as startTime,
                      CAST('3000-01-01' AS TIMESTAMP) as endTime,
                      'Y' as isActive 
              FROM srctemp
              """)

df.createOrReplaceTempView('src')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM src

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE - 1 - MARKING THE UPDATED RECORDS AS EXPRIRED

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO pyspark_cata.source.DimCustomers as trg
# MAGIC USING src as src
# MAGIC ON trg.id = src.id 
# MAGIC AND trg.isActive = 'Y'
# MAGIC
# MAGIC WHEN MATCHED AND src.email <> trg.email
# MAGIC OR src.city <> trg.city 
# MAGIC OR src.country <> trg.country 
# MAGIC OR src.modifiedDate <> trg.modifiedDate
# MAGIC
# MAGIC
# MAGIC THEN UPDATE SET 
# MAGIC trg.endTime = current_timestamp(),
# MAGIC trg.isActive = 'N'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE - 2 - INSERTING NEW + UPDATED RECORDS

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO pyspark_cata.source.DimCustomers as trg
# MAGIC USING src as src
# MAGIC ON trg.id = src.id 
# MAGIC AND trg.isActive = 'Y'
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cata.source.DimCustomers

# COMMAND ----------

