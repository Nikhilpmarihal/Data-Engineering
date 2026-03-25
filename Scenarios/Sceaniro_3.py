# Databricks notebook source
#An e-commerce platform receives customer order details from its mobile application in JSON format through a streaming pipeline. The JSON contains nested fields such as customer information, payment details, and a list of purchased items. To store and analyze this data efficiently in a data warehouse, the nested structure must be flattened into a tabular format using PySpark, ensuring all relevant attributes are readily accessible for reporting and analytics.

# COMMAND ----------

df = spark.read.format("json")\
    .option("inferSchema",True)\
    .option("multiLine",True)\
    .load("/Volumes/pyspark_cata/source/db_volume/jsonData/")
display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Select customer fields and flatten customer structure
df_cust = (
    df.select(
        "customer.customer_id",
        "customer.email",
        "customer.location.city",
        "customer.location.country",
        "*"
    )
    .drop("customer")
)

# Explode delivery_updates and items
df_cust_upd = (
    df_cust
    .withColumn("delivery_updates", explode("delivery_updates"))
    .withColumn("items", explode("items"))
    .select("*")
)

# Display dataframe
display(df_cust)

# COMMAND ----------

