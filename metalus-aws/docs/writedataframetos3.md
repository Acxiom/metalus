[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# Write DataFrame to S3 Parquet
This step group pipeline will write a DataFrame an S3 location. This step group is designed to work with the
[LoadToParquet](../../metalus-common/docs/loadtoparquet.md) pipeline.

## General Information
**Id**: _a8f1acd0-c39b-11eb-b944-4f8822efc9f5_

**Name**: _WriteDataFrameToS3_

## Required Parameters
Required parameters indicated with a *:
* **bronzeZonePath** * - The HDFS path for the root bronze zone folder.
* **fileId** * - The unique id for the file that is processed.
* **credentialName** * - The name of the Secrets Manager key containing the key/secret to use when writing data.
