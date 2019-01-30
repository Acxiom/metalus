# FileManager
The FileManager framework provides an abstraction for working with files across different file systems. The core project
provides two implementations, local and HDFS.

## Functions

* **exists(path)** - Returns a boolean flag if the provided path exists
* **getInputStream(path)** - Returns an InputStream for the path. The path must be a file.
* **getOutputStream(path, append)** - Returns an OutputStream for the path. The path must be a file. The append option is
used to indicate whether the file should be overwritten or appended.
* **rename(path, destPath)** - Renames the provided *path* to the *destPath*.
* **deleteFile(path)** - Deletes the path.

## Implementations

### LocalFileManager
The *LocalFileManager* implementation is used for Spark applications running in client mode.

### HDFSFileManager
The *HDFSFileManager* implementation is used for accessing files stored on the Hadoop file system. A valid *SparkSession*
is required to to use this implementation.