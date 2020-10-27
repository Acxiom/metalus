[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# CSVSteps
This step object provides generic steps for working with CSV. 
The following functions provided:

## Convert CSV String to DataFrame
Given a CSV string, this step will convert it to a DataFrame that can be passed to other steps.
This step is useful for small CSV strings.
Full parameter descriptions listed below:

* **csvString** - The CSV string to convert to a DataFrame.
* **delimiter** - The field delimiter.
* **recordDelimiter - The record delimiter.
* **header** - If true, parse the first "row" as the header.

## Convert CSV String Dataset to DataFrame
Given a Dataset of CSV strings, this step will convert it to a DataFrame that can be passed to other steps. 
Full parameter descriptions listed below:

* **dataset** - The dataset containing CSV strings.
* **dataFrameReaderOptions** - The CSV parsing options.
