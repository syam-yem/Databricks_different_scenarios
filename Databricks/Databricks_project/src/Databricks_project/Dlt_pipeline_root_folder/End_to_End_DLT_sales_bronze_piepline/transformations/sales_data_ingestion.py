import dlt

sales_expectations ={
    "rule1":"sales_id IS NOT NULL"

}
#creating the empty streaming table 

dlt.create_streaming_table(
    name = "sales_stg",
    expect_all_or_drop = sales_expectations
)

@dlt.append_flow(target = "sales_stg")
def east_sales():
    df = spark.readStream.table("dev_catalog.source.sales_east")
    return df


@dlt.append_flow(target = "sales_stg")
def west_sales():
    df = spark.readStream.table("dev_catalog.source.sales_west")
    return df 
