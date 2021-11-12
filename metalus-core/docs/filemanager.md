# FileManager
The FileManager framework provides an abstraction for working with files across different file systems. The core project
provides three implementations, local, HDFS, and SFTP.

## Functions

* **connect()** - Connects to the remote file system.
* **exists(path)** - Returns a boolean flag if the provided path exists
* **getInputStream(path, bufferSize)** - Returns an InputStream for the path. The path must be a file.
* **getOutputStream(path, append, bufferSize)** - Returns an OutputStream for the path. The path must be a file. The append option is
used to indicate whether the file should be overwritten or appended.
* **rename(path, destPath)** - Renames the provided *path* to the *destPath*.
* **deleteFile(path)** - Deletes the path.
* **getSize(path)** - Returns the size in bytes of the remote file
* **getFileListing(path, recursive)** - Returns a list of files on the remote path. supports recursive and simple listings.
* **getDirectoryListing(path)** - Returns a list of directories on the remote path.
* **disconnect()** - Disconnects from the remote file system and releases any resources.
* **copy(input, output, copyBufferSize)** - Copies the contents from the input to the output. This function
does not close the stream.

## Implementations

### LocalFileManager
The *LocalFileManager* implementation is used for Spark applications running in client mode.

### HDFSFileManager
The *HDFSFileManager* implementation is used for accessing files stored on the Hadoop file system.

#### HDFS Parameters
* **conf** - A valid spark conf to extract file system information from.

### SFTPFileManager
The *SFTPFileManager* implementation is used for accessing files via a SFTP connection.

#### SFTP Parameters
* **hostName** - Host name of an sftp server.
* **port** - Optional port. Defaults to 22.
* **user** - Optional user name. Defaults to System.getProperties().get("user.name")
* **password** - Optional password.
* **knownHosts** - Optional path to a known hosts file.
* **bulkRequests** - Optional number of bulk requests. Defaults to 128.
* **config** - Optional map of additional configurations that will get passed to the underlying sftp client.
* **timeout** - Optional timeout. Defaults to 0.
