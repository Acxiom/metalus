[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# S3 to DataFrame
This step group pipeline will load data from an S3 location into a DataFrame. This step 
group is designed to work with the [LoadToParquet](../../metalus-common/docs/loadtoparquet.md) pipeline.

## General Information
**Id**: _ff30fd80-c39b-11eb-b944-4f8822efc9f5_

**Name**: _LoadS3Data_

## Required Parameters
Required parameters indicated with a *:
* **credentialName** * - The name of the Secrets Manager key containing the key/secret to use when writing data.
* **landing_path** * - The HDFS path where the file should be landed
* **fileId** * - The unique id for the file that is processed.
* **readOptions** * - The read options to use when loading this file.
    

