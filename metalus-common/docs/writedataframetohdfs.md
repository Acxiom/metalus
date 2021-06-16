[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Write DataFrame to HDFS Parquet
This step group pipeline will write a DataFrame an HDFS location. This step group is designed to work with the
[LoadToParquet](loadtoparquet.md) pipeline.

## General Information
**Id**: _189328f0-c2c7-11eb-928b-3dca5c59af1b_

**Name**: _WriteDataFrameToHDFS_

## Required Parameters
Required parameters indicated with a *:
* **bronzeZonePath** * - The HDFS path for the root bronze zone folder.
* **fileId** * - The unique id for the file that is processed.
