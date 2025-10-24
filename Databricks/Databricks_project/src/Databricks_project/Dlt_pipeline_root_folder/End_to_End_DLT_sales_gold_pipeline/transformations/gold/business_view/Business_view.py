import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="business_sales_view"
)
def business_sales_view():
    # Read the entire Gold tables as batch
    df_fact = spark.read.table("dev_catalog.gold.fact_sales")
    df_dimcust = spark.read.table("dev_catalog.gold.dim_customer")
    df_dimprod = spark.read.table("dev_catalog.gold.dim_product")

    # Join fact with dimensions
    df_join = (
        df_fact
        .join(df_dimcust, "customer_id", "inner")
        .join(df_dimprod, "product_id", "inner")
    )

    # Select required business columns
    df_prun = df_join.select("region", "category", "total_amount")

    df_agg = df_prun.groupBy("region","category").agg(sum("total_amount").alias("total_sales"))

    return df_prun
