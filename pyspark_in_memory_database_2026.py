#  ---------------------------------------------------------------------------------
# Code updated in 2026 to run on the new free edition of Databricks
# Parquet format now defaults to Delta
# in mem table caching no longer supported on the free edition
# underlying engine no longer based on Scala and JVM, now based on C++ Photon engine
# ------------------------------------------------------------------------------
# This code runs on any test instance of Databricks using pyspark
# I tested it on Databricks Runtime 4.0, using Apache Spark 2.4, but it should
# still run on all the newer releases of Apache Spark since none of the queries
# are particularly complicated and are all based on spark core functionality
# -------------------------------------------------------------------------------
# All the queries are based largely on queries found in the book: 
#       Bill Chambers & Matei Zaharia , 
#       Spark: The Definitive Guide: Big Data Processing Made Simple, 
#       O'Reilly, 2018

# ------------------------------------------------------------------------------
#                    Apache Spark 2.x DataFrames and Python Pyspark
# ------------------------------------------------------------------------------
# ------------------We first create product and customer dataframes-------------
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

# ------------------------------------------------------------------------------
# Create products dataframe
manual_schema_products = StructType([
    StructField('product_id', IntegerType(), False),
    StructField('product_name', StringType(), True),
    StructField('product_specs', StringType(),True),
    StructField('price', FloatType(), True)
])

array_temp1 = [[ 1, 'Apple iPhone 6', '16GB memory', 200.0 ],
                   [ 2, 'Samsung Galaxy 5', '16 megapixel camera', 100.0 ]]

in_memory_products_dataframe = spark.createDataFrame( array_temp1, manual_schema_products)

in_memory_products_dataframe.show(2)
# in_memory_products_dataframe.cache()

# -----------------------------------------------------------------------------
# Create customers dataframe

manual_schema_customers = StructType([
    StructField('customer_id', IntegerType(), False),
    StructField('customer_name', StringType(), True),
    StructField('address', StringType(),True),
    StructField('geolocation_x', FloatType(), True),
    StructField('geolocation_y', FloatType(), True)
])

array_temp2 = [[ 1, 'Pete', 'North Road', 5.0, 10.0 ],
                   [ 2, 'Tanya', 'South Road', 2.0, 0.0 ] ]
in_memory_customers_dataframe = spark.createDataFrame( array_temp2, manual_schema_customers)

in_memory_customers_dataframe.show(2)
# in_memory_customers_dataframe.cache()

# ----------Store permanent copies of the dataframes as tables on disk-------------
# ----------However, this will only work if you first delete the on_disk_tables
# ----------from any previous runs
# ----------parquet is just an efficient big data table format for storage on HDFS
# ----------which Apache Spark can use when making permanent copies of data on disk
# ----------note that Databricks has updated to DeltaLake file format, which is just
# ----------parquet files with a transaction log for catching mid-transaction failures
in_memory_products_dataframe.write.mode("overwrite").saveAsTable("on_disk_table_products")

in_memory_customers_dataframe.write.mode("overwrite").saveAsTable("on_disk_table_customers")
# --------------------------------------------
# ------create temporary in memory customer order data frame
df_orders_temp1 = in_memory_customers_dataframe.select('customer_id',
                                                       'customer_name',
                                                       'address',
                                                       'geolocation_x',
                                                       'geolocation_y')

df_orders_temp1.show(2)
# --------------------------------------------
# ----pull products ordered, the customers who ordered them, and the quantities they orderd
from pyspark.sql.functions import col
from pyspark.sql import functions as F

df_orders_temp2 = df_orders_temp1.withColumn\
    ('product_id',
     F.when(col('customer_id') == 1, 1).\
     when(col('customer_id') == 2, 2))

df_orders_temp2.show(2)

df_orders_temp3 = df_orders_temp2.withColumn\
    ('quantity_ordered',
     F.when(col('customer_id') == 1, 3).\
     when(col('customer_id') == 2, 2))

df_orders_temp3.show(2)

# -------------------------------------------------
#----using function literals in pyspark (this is a uniquely pyspark feature which doesn't appear
#----in spark-sql
from pyspark.sql.functions import lit

df_orders_final = df_orders_temp3.withColumn('warehouse_x', lit(5)).\
                                withColumn('warehouse_y', lit(4)).withColumn('shipping_cost_per_km', lit(10))

df_orders_final.show(2)
# in mem caching is no longer supported for free-tier serverless clusters 
# df_orders_final.cache()
# -------------------------------------------------
                                                
df_inner_joined = df_orders_final.join(in_memory_products_dataframe,
                                       df_orders_final.product_id == in_memory_products_dataframe.product_id)\
                                 .select('customer_id',
                                         'customer_name',
                                         'address',
                                         'geolocation_x',
                                         'geolocation_y',
                                         df_orders_final.product_id,
                                         'quantity_ordered',
                                         'warehouse_x',
                                         'warehouse_y',
                                         'shipping_cost_per_km',
                                         'product_name',
                                         'product_specs',
                                         'price')

df_inner_joined.show(2)
# df_inner_joined.cache()

# ---we need to rename the column here coz of per unit ambiguity ----

df_invoices_temp1 = df_inner_joined.withColumnRenamed('price', 'price_per_unit')

df_invoices_temp1.show(2)

df_invoices_temp2 = df_invoices_temp1.withColumn\
                    ('total_price', df_invoices_temp1.price_per_unit * df_invoices_temp1.quantity_ordered)

df_invoices_temp2.select('customer_name',
                         'product_name',
                         'price_per_unit',
                         'quantity_ordered',
                         'total_price').show(2)

from pyspark.sql.functions import sqrt, pow

df_invoices_temp3 = df_invoices_temp2.withColumn\
                    ('shipping_distance', sqrt(pow(df_invoices_temp2.geolocation_x - \
                                                       df_invoices_temp2.warehouse_x,2) + \
                                               pow(df_invoices_temp2.geolocation_y - \
                                                     df_invoices_temp2.warehouse_y,2)))

df_invoices_temp3.select('customer_name',
                         'geolocation_x',
                         'geolocation_y',
                         'shipping_distance').show(2)

df_invoices_temp4 = df_invoices_temp3.withColumn\
                    ('shipping_costs', df_invoices_temp3.shipping_distance * 10)

df_invoices_temp4.select('customer_name',
                         'geolocation_x',
                         'geolocation_y',
                         'shipping_distance',
                         'shipping_costs').show(2)

df_invoices_final = df_invoices_temp4
# df_invoices_final.cache()

df_invoices_final.select('customer_id',
                         'customer_name',
                         'address',
                         'product_name',
                         'price_per_unit',
                         'quantity_ordered',
                         'total_price',
                         'shipping_costs').show(2)

df_invoices_final.select('*').explain()

# --------------------------------------------------------------  
from pyspark.sql.functions import sum, avg

df_invoices_final.select(sum('quantity_ordered').alias('total_quantity_ordered_aggregate'),
                         avg('shipping_costs').alias('average_shipping_costs'),
                         sum('shipping_costs').alias('total_shipping_costs'),                           
                         sum('total_price').alias('total_revenue_to_shop')).show()
                         
# ------------------------------------------------------------------
permanent_table_name = "on_disk_table_invoices_pyspark"

df_invoices_final.write.mode("overwrite").saveAsTable(permanent_table_name)
# ------------------------------------------------------------------

