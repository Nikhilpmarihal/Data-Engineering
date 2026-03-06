# Databricks notebook source
# MAGIC %sql
# MAGIC -- CREATE TABLE sdp_catalog.source.sales
# MAGIC -- (
# MAGIC --   order_id INT,
# MAGIC --   product_id INT,
# MAGIC --   revenue FLOAT,
# MAGIC --   date DATE,
# MAGIC --   store_id INT
# MAGIC -- );
# MAGIC
# MAGIC INSERT INTO sdp_catalog.source.sales 
# MAGIC VALUES
# MAGIC (7, 1000, 100.00, '2020-01-01', 1),
# MAGIC (8, 1001, 200.00, '2020-01-02', 2)
# MAGIC -- (3, 1002, 300.00, '2020-01-03', 3),
# MAGIC -- (4, 1003, 400.00, '2020-01-04', 4);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.source.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.target.cur_sales_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sdp_catalog.source.sales_north
# MAGIC (
# MAGIC   order_id INT,
# MAGIC   product_id INT,
# MAGIC   revenue FLOAT,
# MAGIC   date DATE,
# MAGIC   store_id INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sdp_catalog.source.sales_north
# MAGIC VALUES
# MAGIC (1, 1000, 100.00, '2020-01-01', 1),
# MAGIC (2, 1001, 200.00, '2020-01-02', 2),
# MAGIC (3, 1002, 300.00, '2020-01-03', 3),
# MAGIC (4, 1003, 400.00, '2020-01-04', 4);
# MAGIC
# MAGIC CREATE TABLE sdp_catalog.source.sales_south
# MAGIC (
# MAGIC   order_id INT,
# MAGIC   product_id INT,
# MAGIC   revenue FLOAT,
# MAGIC   date DATE,
# MAGIC   store_id INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sdp_catalog.source.sales_south
# MAGIC VALUES
# MAGIC (5, 1000, 100.00, '2020-01-01', 1),
# MAGIC (6, 1001, 200.00, '2020-01-02', 2),
# MAGIC (7, 1002, 300.00, '2020-01-03', 3),
# MAGIC (8, 1003, 400.00, '2020-01-04', 4);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.target.total_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### SLOWLY CHANGING DIMENSSIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE sdp_catalog.source.products
# MAGIC -- (
# MAGIC --   product_id INT,
# MAGIC --   product_name STRING,
# MAGIC --   category STRING,
# MAGIC --   subcategory STRING,
# MAGIC --   updated_at TIMESTAMP
# MAGIC -- );
# MAGIC
# MAGIC INSERT INTO sdp_catalog.source.products VALUES
# MAGIC -- (1001, 'Product 1', 'Category 1', 'Subcategory 1', current_timestamp()),
# MAGIC -- (1002, 'Product 2', 'Category 2', 'Subcategory 2', current_timestamp()),
# MAGIC ( null,'Product 3', 'Category 4', 'Subcategory 4', current_timestamp());
# MAGIC -- (null, 'Product 4', 'Category 4', 'Subcategory 4', current_timestamp());
# MAGIC
# MAGIC SELECT * FROM sdp_catalog.source.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.target.products_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.target.products_scd1

# COMMAND ----------

