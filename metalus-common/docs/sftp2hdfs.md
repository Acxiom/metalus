[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# SFTP to HDFS File copy
This step group pipeline will copy a file from an SFTP location to an HDFS location and then load the data to a 
DataFrame. The DataFrame will be loaded against the downloaded file and does not support encrypted files.

## General Information
**Id**: _46f5e310-4c47-11ea-a0a7-a749c3ebbd62_

**Name**: _SG_SftpToHdfs_

## Required Parameters
Required parameters are indicated with a *:
* **sftp_host** * - The host name/ip of the SFTP server
* **sftp_username** * - The username of the SFTP server
* **sftp_password** * - The password of the SFTP server
* **sftp_port** - The optional SFTP port. Defaults to 22
* **sftp_input_path** * - The path to the file on the SFTP server
* **landing_path** * - The HDFS path where the file should be landed 
* **input_buffer_size** - The size of the buffer for the input stream. Defaults to 65536
* **output_buffer_size** - The size of the buffer for the output stream. Defaults to 65536
* **read_buffer_size** - The size of the buffer used to transfer from input to output. Defaults to 32768
