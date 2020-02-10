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

* **dataFrame** - The DataFrame to persist
* **storageLevel** - A string name of the storage level desired

## Unpersist DataFrame
Mark the DataFrame as non-persistent and and remove all blocks for it from memory and disk. Full parameter descriptions
listed below:

* **dataFrame** - The DataFrame to unpersist
* **blocking** - Flag indicating whether the operation should block
