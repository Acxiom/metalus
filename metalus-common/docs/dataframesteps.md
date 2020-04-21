[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# DataFrameSteps
This step object builds DataFrameReader and DataFrameWriter objects. 
There are two step functions provided:

## Get DataFrameReader
Given a DataFrameReaderOptions object, this step will build a DataFrameReader.
Full parameter descriptions are listed below:

* **dataFrameReaderOptions** - DataFrameReaderOptions object used to configure the DataFrameReader.

## Get DataFrameWriter
Given a DataFrameWriterOptions object, this step will build a DataFrameWriter[Row].
Full parameter descriptions are listed below:

* **dataFramewriterOptions** - DataFrameWriterOptions object used to configure the DataFrameWriter.

## Persist DataFrame
Given a DataFrame object and optional storage level, this step will persist the data. Full parameter descriptions
listed below:

* **dataFrame** - The DataFrame to persist.
* **storageLevel** - A string name of the storage level desired.

## Unpersist DataFrame
Mark the DataFrame as non-persistent and and remove all blocks for it from memory and disk. Full parameter descriptions
listed below:

* **dataFrame** - The DataFrame to unpersist.
* **blocking** - Flag indicating whether the operation should block.

## Repartition DataFrame
Repartition the dataFrame to have the provided number of partitions. Full parameter descriptions listed below:

* **dataFrame** - The DataFrame to repartition.
* **partitions** - The desired number of partitions.
* **rangePartition** - Optional flag to indicate whether partitionByRange should be used. Defaults to false.
* **shuffle** - Optional flag to indicate whether a shuffle needs to be performed. Defaults to true.
* **partitionExpressions** - Optional list of expressions used to sort data into partitions.

## Sort DataFrame
Sort the DataFrame based on the provided list of expressions. Full parameter descriptions listed below:

* **dataFrame** - The DataFrame to sort.
* **expressions** - The List of sort expressions.
* **descending** - Optional flag to indicate whether sort order should be descending. 
When true, will apply the desc function to each provided expression. Defaults to false.
