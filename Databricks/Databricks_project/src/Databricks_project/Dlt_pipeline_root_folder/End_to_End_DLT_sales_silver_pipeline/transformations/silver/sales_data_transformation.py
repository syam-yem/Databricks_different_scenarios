import dlt
from pyspark.sql.functions import *

@dlt.view(
    name ="sales_view_trns"
)
def sales_view_trns():
    df = spark.readStream.table("dev_catalog.bronze.sales_stg")
    df = df.withColumn("total_amount",col("quantity")* col("amount"))
    return df 

dlt.create_streaming_table(
    name = "dev_catalog.silver.sales_silver"
)


dlt.create_auto_cdc_flow(
    target = "dev_catalog.silver.sales_silver",
    source = "sales_view_trns",
    keys = ["sales_id"],
    sequence_by="sale_timestamp",
    ignore_null_updates= False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None

)

