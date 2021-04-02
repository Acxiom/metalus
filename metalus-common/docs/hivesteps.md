[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# HiveSteps
This step object provides a way to read from and write to Hive. 
To use these, hive support should be enabled on the spark context. The There are two step functions provided:

## Write DataFrame
This function will write a given DataFrame to a Hive table. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to HDFS.
* **table** - The Hive table name.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter.

## Read DataFrame
This function will read a hive table into a DataFrame. Full parameter descriptions are listed below:

* **table** - The Hive table name.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader.

## Drop Hive Object
This function will perform a drop operation.
Toggles are available to control casecade and "If exists" behavior.
* **name** - The Hive object to drop.
* **objectType** - The type of object to drop. Default value is *TABLE*.
* **ifExists** - Boolean flag that, when true, will prevent an error from being raised if the object name is not found.
Default value is *false*.
* **cascade** - Boolean flag to toggle cascading deletion behavior. Default value is *false*.

## Create Table
This function will create a table, managed or external, based on the provided options.
By default, the format will be "hive".
* **name** - The table name.
* **externalPath** - Optional path of the external table. If not provided, the table will be manged by the meta store.
* **options** - Optional DataFrameReaderOptions providing the format, schema, and other options for the table.

## Database Exists
This function will check if a given database exists
* **name** - The database name.

## Table Exists
This function will check if a given database exists
* **name** - The table name.
* **database** - Optional database name

## Set Current Database
This function will set the default database for the current spark session to the provided database name.
* **name** - The database name.
