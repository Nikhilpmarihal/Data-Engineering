# from pyspark import pipelines as dp
# from pyspark.sql.functions import * 


# # Create empty streaming table SCD2
# dp.create_streaming_table("products_scd2")

# # Create empty streaming table SCD1
# dp.create_streaming_table("products_scd1")

# # Streaming view source 
# @dp.temporary_view
# def products_source():
#     df = spark.readStream.table("sdp_catalog.source.products")
#     return df

# # SCD TYPE 2
# dp.create_auto_cdc_flow(
#   target = "products_scd2",
#   source = "products_source",
#   keys = ["product_id"],
#   sequence_by = col("updated_at"),
#   except_column_list = ["updated_at"],
#   stored_as_scd_type = "2"
# )

# # SCD TYPE 1
# dp.create_auto_cdc_flow(
#   target = "products_scd1",
#   source = "products_source",
#   keys = ["product_id"],
#   sequence_by = col("updated_at"),
#   except_column_list = ["updated_at"],
#   stored_as_scd_type = "1"
# )
