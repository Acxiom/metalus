[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Data Options
Read and write options need to be easy to configure and reusable across steps and pipelines. The DataFrameReaderOptions
and DataFrameWriterOptions are provided as a way to easily model reusable instructions.

## DataFrame Reader Options
This object represents options that will be translated into Spark settings at read time.

* *format* - The file format to use. Defaulted to _parquet_.
* *schema* - An optional _com.acxiom.pipeline.steps.Schema_ which will be applied to the DataFrame.
* *options* - Optional map of settings to pass to the underlying Spark data source.
* *streaming* - Boolean that indicates if this object should build a streaming DataFrame. Default is false.
Example:
```json
{
  "format": "csv",
  "options": {
    "header": "true",
    "delimiter": ","
  },
  "schema": {
    "attributes": [
      {
        "name": "CUSTOMER_ID",
        "dataType": {
          "baseType": "Integer"
        }
      },
      {
        "name": "FIRST_NAME",
        "dataType": {
          "baseType": "String"
        }
      }
    ]
  }
}
```
## DataFrame Writer Options
This object represents options that will be translated into Spark settings at write time.

* *format* - The file format to use. Defaulted to _parquet_.
* *saveMode* - The mode when writing a DataFrame. Defaulted to "Overwrite"
* *options* - Optional map of settings to pass to the underlying Spark data source.
* *bucketingOptions* - Optional BucketingOptions object for configuring Bucketing
* *partitionBy* - Optional list of columns for partitioning.
* *sortBy* - Optional list of columns for sorting.
Example:
```json
{
  "format": "parquet",
  "saveMode": "Overwrite",
  "bucketingOptions": {},
  "options": {
    "escapeQuotes": false
  },
  "schema": {
    "attributes": [
      {
        "name": "CUSTOMER_ID",
        "dataType": {
          "baseType": "Integer"
        }
      },
      {
        "name": "FIRST_NAME",
        "dataType": {
          "baseType": "String"
        }
      }
    ]
  }
}
```
## Bucketing Options
This object represents the options used to bucket data when it is being written by Spark.

* *numBuckets* - The number of buckets (partition) to build.
* *columns* - A list of columns to bucket by.
