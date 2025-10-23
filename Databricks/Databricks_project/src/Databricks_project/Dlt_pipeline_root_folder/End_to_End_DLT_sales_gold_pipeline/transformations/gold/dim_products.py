import dlt

dlt.create_streaming_table(
    name ="dev_catalog.gold.dim_product"
)
##auto_cdc_flow

dlt.create_auto_cdc_flow(
    target = "dim_product",
    source = "dev_catalog.silver.product_view_trns",
    keys = ["product_id"],
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