[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# S3 Load to Bronze
This execution template will load data from an SFTP location into a DataFrame using the [DownloadSFTPToHDFSWithDataFrame](downloadsftptohdfswithdataframe.md), call
the [LoadToParquet](../../metalus-common/docs/loadtoparquet.md) pipeline, and use the [WriteDataFrameToHDFS](writedataframetohdfs.md)
pipeline to store the data in the bronze zone (HDFS) as parquet.

## General Information
**Id**: _load_data_bronze_sftp_hdfs_

**Name**: _Load SFTP to Bronze HDFS_

## Form
A custom form allows configuring the download and input parameters as well as controlling the pipeline behavior.
