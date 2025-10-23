import dlt

customers_expectation ={
    "rule1":"customer_id is not null",
    "rule2":"customer_name is not null"
}

@dlt.table(
    name = "customers_stg"
)
@dlt.expect_all_or_drop(customers_expectation)

def customers_stg():
    df = spark.readStream.table("dev_catalog.source.customers")
    return df

