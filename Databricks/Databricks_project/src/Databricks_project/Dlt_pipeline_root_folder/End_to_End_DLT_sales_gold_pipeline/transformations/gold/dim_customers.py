import dlt


@dlt.view()
def customer_gold_temp_view():
    df = spark.readStream.table("dev_catalog.silver.customers_silver")
    return df 


dlt.create_streaming_table(
    name ="dim_customer"
)
##auto_cdc_flow

dlt.create_auto_cdc_flow(
    target = "dim_customer",
    source = "customer_gold_temp_view",
    keys = ["customer_id"],
    sequence_by="last_updated",
    ignore_null_updates= False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None
)