[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# FileManagerSteps
This object provides steps for working with FileManager implementations.

## Copy
This step will copy the contents from the source path using the source FileManager to the
destination path using the destination FileManager using input and output streams. Full parameter descriptions are 
listed below:

* **srcFS** - The source FileManager used to gain access to the data.
* **srcPath** - The path on the source file system containing the data to copy.
* **destFS** - The destination FileManger used to write the output data.
* **destPath** - The path on the destination file system to write the data.

## Disconnect File Manager
This step provides an easy way to disconnect the FileManager and free any resources. Full parameter descriptions are 
listed below:

* **fileManager** - The FileManager to disconnect. 
