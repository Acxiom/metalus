[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Load to Bronze
This pipeline will load source data to the _bronze_ zone. During the load the pipeline will optionally standardize
column names, add a unique record id and a static file id column.

## General Information
**Id**: _a9f62840-2827-11ec-9c0c-cbf3549779e5_

**Name**: _LoadToBronze_

## Required Parameters (all parameters should be part of the globals)
* **sourceBronzeConnector** - The data connector to use as the source.
* **sourceBronzePath** - The path to get the source data.
* **sourceBronzeReadOptions** - The [options](dataoptions.md#dataframe-reader-options) that describe the source data and any settings to use during the read.
* **executeColumnCleanup** - Optional boolean indicating that column names should be standardized. _Defaults to true._
* **addRecordId** - Optional boolean indicating that a unique record id should be added. The uniqueness is only within 
  the current source data and does not consider the destination data. _Defaults to true._
* **addFileId** - Optional boolean indicating that a static column containing the provided _fileId_ should be added to
  each record. _Defaults to true._
* **destinationBronzeConnector** - The data connector to use as the destination.
* **destinationBronzePath** - The path to write the data.
* **destinationBronzeWriteOptions** - The [options](dataoptions.md#dataframe-writer-options) to use during the write.

## Streaming
This pipeline can be used with streaming connectors. By default, if a streaming connector is provided as the load connector,
the job will run indefinitely. The [Streaming Query Monitor](streamingquerymonitor.md) provides additional options for
writing [partitioned data](streamingquerymonitor.md#batchpartitionedstreamingquerymonitor-_comacxiompipelinestreamingbatchpartitionedstreamingquerymonitor_).
