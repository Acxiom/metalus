[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Download File and Store in Bronze Zone
This step group pipeline will copy a file from an SFTP location to an HDFS location (using pipeline [SG_SftpToHdfs](sftp2hdfs.md)
as a step-group step).  It then parses the new data, performs some basic maintenance (standardize column names, adds a record id 
and the file id to each row) and stores it in a Parquet datastore.

## General Information
**Id**: _f4835500-4c4a-11ea-9c79-f31d60741e3b_

**Name**: _DownloadToBronzeHdfs_

## Required Parameters
Required parameters are indicated with a *:
* **sftpHost** * - The host name/ip of the SFTP server
* **sftpUsername** * - The username of the SFTP server
* **sftpPassword** * - The password of the SFTP server
* **sftpPort** - The optional SFTP port. Defaults to 22
* **sftpInputPath** * - The path to the file on the SFTP server
* **landingPath** * - The HDFS path where the file should be landed 
* **inputBufferSize** - The size of the buffer for the input stream. Defaults to 65536
* **outputBufferSize** - The size of the buffer for the output stream. Defaults to 65536
* **readBufferSize** - The size of the buffer used to transfer from input to output. Defaults to 32768
* **inputReaderOptions** * - The DataFrameReader options for the selected input file.
* **bronzeZonePath** * - The HDFS path for the root bronze zone folder
* **fileId** * - The unique id for the file being processed.

