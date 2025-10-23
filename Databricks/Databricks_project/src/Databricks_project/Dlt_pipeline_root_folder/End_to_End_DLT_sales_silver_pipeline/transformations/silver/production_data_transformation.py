import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name ="product_view_trns"
)
def product_view_trns():
    df = spark.readStream.table("dev_catalog.bronze.product_stg")
    df = df.withColumn("price",col("price").cast(IntegerType()))
    return df 

dlt.create_streaming_table(
    name = "dev_catalog.silver.products_silver"
)


dlt.create_auto_cdc_flow(
    target = "dev_catalog.silver.products_silver",
    source = "product_view_trns",
    keys = ["product_id"],
    sequence_by="last_updated",
    ignore_null_updates= False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None

)

