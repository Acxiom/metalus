[Documentation Home](readme.md)

# FileManager
The FileManager framework provides an abstraction for working with files across different file systems. Steps can use
this to perform operations against files which help make decisions, perform cleanup and copy files to and from a remote 
file system.

## Functions
Several functions are provided which allow a consistent experience across file systems.

* **connect()** - Connects to the remote file system.
* **exists(path)** - Returns a boolean flag if the provided path exists
* **getInputStream(path, bufferSize)** - Returns an InputStream for the path. The path must be a file.
* **getOutputStream(path, append, bufferSize)** - Returns an OutputStream for the path. The path must be a file. The append option is
used to indicate whether the file should be overwritten or appended.
* **rename(path, destPath)** - Renames the provided *path* to the *destPath*.
* **deleteFile(path)** - Deletes the path.
* **getSize(path)** - Returns the size in bytes of the remote file
* **getFileListing(path)** - Returns a list of files on the remote path.
* **disconnect()** - Disconnects from the remote file system and releases any resources.
* **copy(input, output, copyBufferSize, closeStreams)** - Copies the contents from the input to the output. This function
will close the streams if the closeStreams boolean is set to true. Default is false.

## Implementations
The core project provides two implementations, **local** and **HDFS**.

### LocalFileManager
The *LocalFileManager* implementation is used for Spark applications running in client mode.

### HDFSFileManager
The *HDFSFileManager* implementation is used for accessing files stored on the Hadoop file system. A valid *SparkConf*
is required to to use this implementation.
