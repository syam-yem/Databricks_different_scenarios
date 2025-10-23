import dlt

dlt.create_streaming_table(
    name ="dev_catalog.gold.fact_sales"
)
##auto_cdc_flow

dlt.create_auto_cdc_flow(
    target = "fact_sales",
    source = "dev_catalog.silver.sales_view_trns",
    keys = ["sales_id"],
    sequence_by="sale_timestamp",
    ignore_null_updates= False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None
)