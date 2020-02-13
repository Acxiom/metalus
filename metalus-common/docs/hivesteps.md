[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# HiveSteps
This step object provides a way to read from and write to Hive. 
To use these, hive support should be enabled on the spark context. The There are two step functions provided:

## Write DataFrame
This function will write a given DataFrame to a Hive table. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to HDFS.
* **table** - The Hive table name.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

## Read DataFrame
This function will read a hive table into a DataFrame. Full parameter descriptions are listed below:

* **table** - The Hive table name
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader
