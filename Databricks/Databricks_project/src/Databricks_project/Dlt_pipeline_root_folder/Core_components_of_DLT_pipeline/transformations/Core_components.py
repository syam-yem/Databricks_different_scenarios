###Streaming Tables

import dlt

@dlt.table(
    name = "first_streaming_table"
)
def first_streaming_table():
    df = spark.readStream.table("dev_catalog.silver.factstream")
    return df


###Materialised view
###Mat view is the result of the query saved somewhere in the destination
@dlt.table(
    name ="first_materialized_view"
)
def first_materialized_view():
    df = spark.read.table("dev_catalog.silver.factstream")
    return df 



###batch_view
@dlt.view(
    name="firstbatch_view"
)
def firstbatch_view():
    df = spark.read.table("dev_catalog.silver.factstream")
    return df


#####streaming view
@dlt.view(
    name="first_streaming_view"
)
def first_streaming_view():
    df = spark.readStream.table("dev_catalog.silver.factstream")
    return df 











