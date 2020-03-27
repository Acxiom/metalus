[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# SFTPSteps
This object provides step functions useful for working with an SFTP server.

## Create File Manager
This step will return a FileManager implementation that is useful for working with an SFTP server. Full parameter 
descriptions are listed below:

* **hostName** - The host name of the remote server
* **username** - Optional username used to authenticate
* **password** - Optional password used to authenticate
* **port** - Optional port number. Default is 22.
* **strictHostChecking** - Optional flag used to enable/disable strict host checking. Default is true.
