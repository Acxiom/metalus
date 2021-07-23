[Documentation Home](../../docs/readme.md) | [GCP Home](../readme.md)

# BigQuerySteps
BigQuerySteps provides steps that allow a reading a DataFrame and writing a DataFrame to a BigQuery Table.

## Write to Table
This function will write a given DataFrame to the provided table. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to GCS.
* **table** - A BigQuery project, dataset and table path where the data will be written.
* **tempBucket** - A GCS path where the data will be written temporarily. The bucket should be part of the path.
* **credentials** - The credential map to use or none to use the system provided credentials.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

## Read From Table
This function will read data from the provided table into a DataFrame. Full parameter descriptions are listed below:

* **table** - A BigQuery project, dataset and table path where the data will be read.
* **credentials** - The credential map to use or none to use the system provided credentials.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader
