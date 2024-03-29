[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# FileManagerSteps
This object provides steps for working with [FileManager](../../docs/filemanager.md) implementations.

## Copy
These steps will copy the contents from the source path using the source FileManager to the
destination path using the destination FileManager using input and output streams. Full parameter descriptions are 
listed below:

### Auto Buffering
* **srcFS** - The source FileManager used to gain access to the data.
* **srcPath** - The path on the source file system containing the data to copy.
* **destFS** - The destination FileManger used to write the output data.
* **destPath** - The path on the destination file system to write the data.
### Basic Buffering
Includes the parameters for auto buffering but exposes the input and output buffer sizes:
* **inputBufferSize** - The size of the buffer to use for reading data during copy
* **outputBufferSize** - The size of the buffer to use for writing data during copy
### Advanced Buffering
Includes the parameters for basic buffering but exposes the copy buffer size:
* **copyBufferSize** - The intermediate buffer size to use during copy

## Compare File Sizes
This step will compare the size of the source and destination files and return -1 if the source file size is smaller
than the destination file size, 0 if they are the same and 1 if the source file size is larger than the source file
size.

* **srcFS** - The source FileManager.
* **srcPath** - The path on the source file system to the file.
* **destFS** - The destination FileManger.
* **destPath** - The path on the destination file system to the file.

## Get an InputStream
This step will get an input stream.

* **fileManager** - The FileManager.
* **path** - The path to the file being read.
* **bufferSize** - The optional buffer size. Uses the file manager's default if not provided.

## Get an OutputStream
This step will get an Output stream.

* **fileManager** - The FileManager.
* **path** - The path to the file being written.
* **append** - The optional append flag. Uses the file manager's default if not provided.
* **bufferSize** - The optional buffer size. Uses the file manager's default if not provided.

## Rename a File
This step will rename a file.

* **fileManager** - The FileManager.
* **path** - The path to the file being renamed.
* **destPath** - The destination path.

## Get File Size
This step will return the size of a file.

* **fileManager** - The FileManager.
* **path** - The path to the file being checked.

## Does File Exist
This step will return whether a file exists.

* **fileManager** - The FileManager.
* **path** - The path to the file being checked.

## Get a File Listing
This step will return a file listing.

* **fileManager** - The FileManager.
* **path** - The path to list.
* **recursive** - Indicates whether to perform a recursive listing. Uses the file manager's default if not provided.

## Get a Directory Listing
This step will return a file listing.

* **fileManager** - The FileManager.
* **path** - The path to list.

## Delete File
This step will delete a file.

* **fileManager** - The FileManager.
* **path** - The path to the file being deleted.
## Disconnect File Manager
This step provides an easy way to disconnect the FileManager and free any resources. Full parameter descriptions are 
listed below:

* **fileManager** - The FileManager to disconnect. 

## Create a FileManager
This step will create a FileManager using the provided [FileConnector](../../docs/fileconnectors.md).

Full parameter descriptions are listed below:

* **fileConnector** - The FileConnector with implementation information.
