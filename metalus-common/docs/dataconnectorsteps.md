[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# DataConnectorSteps
This step object provides a way to load from and write using DataConnectors. There are several step functions provided:

## Write DataFrame
This function will write a given DataFrame using the provided DataConnector. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written.
* **connector** - The DataConnector to use when writing data.
* **destination** - An optional destination.
* **options** - Optional DataFrameWriterOptions object to configure the DataFrameWriter

## Load DataFrame
This function will load a DataFrame using the provided DataConnector. Full parameter descriptions are listed below:

* **source** - An optional source.
* **connector** - The DataConnector to use when loading data.
* **options** - Optional DataFrameReaderOptions object to configure the DataFrameReader
