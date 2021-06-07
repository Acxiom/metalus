[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# SFTP to HDFS File copy
This step group pipeline will copy a file from an SFTP location to an HDFS location and then load the data to a 
DataFrame. The DataFrame will be loaded against the downloaded file and does not support encrypted files. This step 
group works with the [LoadToParquet](loadtoparquet.md) pipeline.

## General Information
**Id**: _e9ce4710-beda-11eb-977b-1f7c49e5a75d_

**Name**: _DownloadSFTPToHDFSWithDataFrame_

## Required Parameters
Required parameters indicated with a *:
* **sftp_host** * - The host name/ip of the SFTP server
* **sftp_username** * - The username of the SFTP server
* **sftp_password** * - The password of the SFTP server
* **sftp_port** - The optional SFTP port. Defaults to 22
* **sftp_input_path** * - The path to the file on the SFTP server
* **input_buffer_size** - The size of the buffer for the input stream. Defaults to 65536
* **output_buffer_size** - The size of the buffer for the output stream. Defaults to 65536
* **read_buffer_size** - The size of the buffer used to transfer from input to output. Defaults to 32768
* **landing_path** * - The HDFS path where the file should be landed
* **fileId** * - The unique id for the file that is processed.
* **readOptions** - The reader options to use when loading the data from disk.
