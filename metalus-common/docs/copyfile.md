[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# Copy File
This pipeline will copy a file from one location to another. The download will make up to 5 attempts when a failure
occurs. A verification check will ensure the downloaded file is the same size and if not, will re-download up to 5 times.

## General Information
**Id**: _43bc9450-2689-11ec-9c0c-cbf3549779e5_

**Name**: _CopyFile_

## Required Parameters
* **sourceConnector** - The file connector to use as the source. This is expected to be in the globals.
* **sourceCopyPath** - The path to get the source file. This is expected to be in the globals.
* **destinationConnector** - The file connector to use as the destination. This is expected to be in the globals.
* **destinationCopyPath** - The path to write the destination file. This is expected to be in the globals.
