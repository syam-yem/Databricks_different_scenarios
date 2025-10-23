import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="Staging_orders"
)
def Staging_orders():
    df = spark.readStream.table("dev_catalog.bronze.order_01")
    return df 

###creating a transformed dependency table 
@dlt.table(
    name="transformed_orders"
)
def transformed_orders():
    df = spark.readStream.table("dev_catalog.silver.Staging_orders")
    df = df.withColumn("order_status",lower(col("order_status")))
    return df 

###creating an aggregated table 

@dlt.table(
    name="aggregated_orders"
)
def aggregated_orders():
    df =spark.readStream.table("transformed_orders")
    df = df.groupBy("order_status").count()
    return df 


