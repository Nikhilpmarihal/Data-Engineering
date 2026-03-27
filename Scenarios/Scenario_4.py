# Databricks notebook source
# In a large-scale data processing project, multiple PySpark notebooks require the same set of custom transformation functions, such as date formatting, null handling, and data validation. Instead of duplicating the code across notebooks, a Python class is created to store these reusable functions. This ensures consistency, reduces maintenance effort, and improves code readability across the project.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

class DataValidation:

    def __init__(self,df):
        self.df = df

    def dedup(self,keyCol,cdcCol):
        df = self.df.withColumn("dedup",row_number().over(Window.partitionBy(keyCol).orderBy(desc(cdcCol))))
        df = df.filter(col("dedup") == 1).drop("dedup")
        return df
    
def removeNulls(self,nullCol):
    df = self.df.filter(col(nullCol).isNotNull())
    return df


# COMMAND ----------

df = spark.createDataFrame(
    [
        ("1", "2020-01-01", 100),
        ("2", "2020-01-02", 200),
        ("3", "2020-01-03", 150),
        ("4", "2020-01-04", 300),
        ("5", "2020-01-05", 250),
        ("6", "2020-01-12", 190),
        ("7", "2020-01-07", 220),
        ("8", "2020-01-08", 275),
        ("9", "2020-01-09", 320),
        ("10", "2020-01-10", 400),
        ("3", "2020-01-03", 150) 
    ],
    ["order_id", "order_date", "amount"]
)

display(df)

# COMMAND ----------

cls_obj = DataValidation(df) # Instance

# COMMAND ----------

df_dedup = cls_obj.dedup("order_id","order_date")

# COMMAND ----------

display(df_dedup)

# COMMAND ----------

