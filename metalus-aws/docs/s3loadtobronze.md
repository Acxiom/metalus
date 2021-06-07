[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# S3 Load to Bronze
This execution template will load data from an S3 location into a DataFrame using the [LoadS3Data](loads3data.md), call
the [LoadToParquet](../../metalus-common/docs/loadtoparquet.md) pipeline, and use the [WriteDataFrameToS3](writedataframetos3.md)
pipeline to store the data in the bronze zone as parquet.

## General Information
**Id**: _s3_load_data_bronze_

**Name**: _S3 Load to Bronze_

## Form
A custom form allows configuring the input parameters as well as controlling the pipeline behavior.
