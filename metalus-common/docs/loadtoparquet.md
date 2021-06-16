[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Download File and Store in Bronze Zone
This pipeline will parse incoming data, perform some basic maintenance (standardize column names, adds a record id
and the file id to each row) and stores it in a Parquet datastore. The load step is a dynamic step group which uses the
pipelineId defined by the global parameter: _loadDataFramePipelineId_. The write step is a dynamic step group which uses
the pipelineId defined by the global parameter: _writeDataFramePipelineId_. The write step group will be passed a global
named _inputDataFrame_.


## General Information
**Id**: _dcff1d10-c2c3-11eb-928b-3dca5c59af1b_

**Name**: _DownloadToBronzeHdfs_

## Required Parameters
Required parameters indicated with a *:
* **loadDataFramePipelineId** * - The step group pipeline to use when loading the data into a DataFrame.
* **writeDataFramePipelineId** * - The step group pipeline to use when writing the data to the bronze zone.
* **bronzeZonePath** * - The HDFS path for the root bronze zone folder.
* **fileId** * - The unique id for the file that is processed.
