import dlt

prod_expectations = {
"rule1":"product_id is not null",
"rule2":"price >=0"    
}


@dlt.table(
    name="product_stg"
)
@dlt.expect_all_or_drop(prod_expectations)
def products_stg():
    df = spark.readStream.table("dev_catalog.source.products")
    return df 

    