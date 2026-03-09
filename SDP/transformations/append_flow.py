# from pyspark import pipelines as dp
# from pyspark.sql.functions import * 

# # Creating Empty streaming table
# dp.create_streaming_table("total_sales")

# # Appending north sales to the total sales
# @dp.append_flow(target = "total_sales")
# def north_sales():
#     df = spark.readStream.table("sdp_catalog.source.sales_north")
#     return df

# # Appending south sales to the total sales
# @dp.append_flow(target = "total_sales")
# def south_sales():
#     df = spark.readStream.table("sdp_catalog.source.sales_south")
#     return df
