[Home](../readme.md)

# Connectors
The _Connectors_ API provides an abstraction for accessing data that can be configured at runtime further enhancing
the ability to build reusable [pipelines](pipelines.md) and [steps](steps.md). Each connector type abstracts the details
required to read/write the data allowing entire applications to be built without knowledge of the source/destination
until runtime. There are three _types_ of connector:
* FILE
* DATA
* STREAM

Connector implementations provide read and write windowing functions (_getReader_, _getWriter_). The _DataRowReader_ can
be used to window data from a source while the _DataRowWriter_ can be used to write windows of data to a destination. This
approach allows data to be read from a _STREAM_ connector (Kafka, Kinesis, GCP Pub/Sub, etc.) and written to a _DATA_ connector
such as a JDBC destination. The _FlowUtilsSteps.streamData_ provides a mechanism for reading data from a source and processing
the data using [Data References](data-references.md) to perform transformations prior to writing the data to another
connector.

Implementations that do not support windowing will return _None_ instead of a _DataRowReader/DataRowWriter_.

* [File Connectors](#file-connectors)
* [File Connector Steps](#file-connector-steps)
  * [Get File Manager](#get-file-manager)
  * [Copy](#copy)
  * [Compare File Sizes](#compare-file-sizes)
  * [Delete File](#delete-file)
  * [Get Input Stream](#get-input-stream)
  * [Get Output Stream](#get-output-stream)
  * [Rename](#rename)
  * [Get Size](#get-size)
  * [Exists](#exists)
  * [Get File Listing](#get-file-listing)
  * [Get Directory Listing](#get-directory-listing)
  * [Read Header](#read-header)
* [Data Connectors](#data-connectors)
* [Stream Connectors](#stream-connectors)
* [Connector Steps](#connector-steps)
* [Example Configurations](#example-configurations)
  * [Local File Connector](#local-connector)
  * [SFTP File Connector](#sftp-connector)
  * [JDBC Data Connector](#jdbc-dataconnector)

## File Connectors
File connectors are designed to work with data represented on a file system such as local, SFTP, S3 and GCS. File connectors
provide the _getFileManager_ method which provides the ability to manipulate files. A set of steps are provided which allow
working with files all located within the _FileManagerSteps_ object.
## File Connector Steps
### Get File manager
This step takes a _FILE_ connector and returns a _FileManager_ to be used by the other
steps. It is recommended that the **disconnectFileManager** step be called to perform any cleanup once
the _FileManager_ is no longer needed.
#### Steps
| Package                  | Object           | Step                  | Id                                    |
|--------------------------|------------------|-----------------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getFileManager        | 259a880a-3e12-4843-9f02-2cfc2a05f576  |
| com.acxiom.metalus.steps | FileManagerSteps | disconnectFileManager | 3d1e8519-690c-55f0-bd05-1e7b97fb6633  |
#### Parameters
| Name             | Type          | Required | Step                  |
|------------------|---------------|----------|-----------------------|
| fileConnector    | FileConnector | Y        | getFileManager        |
| fileManager      | FileManager   | Y        | disconnectFileManager |
### Copy
There are three copy steps, but it is recommended that the **copy-file pipeline** be used as opposed to using the steps.
This pipeline will attempt to copy a file from the source to the destination, verify the results and retry up to 5 times.
When this pipeline isn't sufficient, then there are three copy steps available.
#### Steps
| Package                  | Object           | Step      | Id                                    |
|--------------------------|------------------|-----------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | copy      | 0342654c-2722-56fe-ba22-e342169545af  |
| com.acxiom.metalus.steps | FileManagerSteps | copy      | c40169a3-1e77-51ab-9e0a-3f24fb98beef  |
| com.acxiom.metalus.steps | FileManagerSteps | copy      | f5a24db0-e91b-5c88-8e67-ab5cff09c883  |
#### Parameters
| Name             | Type        | Required | Step |
|------------------|-------------|----------|------|
| srcFS            | FileManager | Y        | All  |
| srcPath          | String      | Y        | All  |
| destFS           | FileManager | Y        | All  |
| destPath         | String      | Y        | All  |
| inputBufferSize  | Int         | N        | 2,3  |
| outputBufferSize | Int         | N        | 2,3  |
| copyBufferSize   | Int         | N        | 3    |
 ### Compare File Sizes
This step will compare teh file size of the source file to the destination file.
#### Steps
| Package                  | Object           | Step             | Id                                    |
|--------------------------|------------------|------------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | compareFileSizes | 1af68ab5-a3fe-4afb-b5fa-34e52f7c77f5  |
#### Parameters
| Name             | Type        | Required |
|------------------|-------------|----------|
| srcFS            | FileManager | Y        |
| srcPath          | String      | Y        |
| destFS           | FileManager | Y        |
| destPath         | String      | Y        |
### Delete File
This step will delete a file.
#### Steps
| Package                  | Object           | Step       | Id                                    |
|--------------------------|------------------|------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | deleteFile | bf2c4df8-a215-480b-87d8-586984e04189  |
#### Parameters
| Name          | Type        | Required |
|---------------|-------------|----------|
| fileManager   | FileManager | Y        |
| path          | String      | Y        |
### Get Input Stream
This step will get an input stream from the specified file.
#### Steps
| Package                  | Object           | Step           | Id                                    |
|--------------------------|------------------|----------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getInputStream | 5d59b2e8-7f58-4055-bf82-0d17c5a79a17  |
#### Parameters
| Name          | Type        | Required |
|---------------|-------------|----------|
| fileManager   | FileManager | Y        |
| path          | String      | Y        |
| bufferSize    | Int         | N        |
### Get Output Stream
This step will get an output stream for a destination file.
#### Steps
| Package                  | Object           | Step            | Id                                    |
|--------------------------|------------------|-----------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getOutputStream | 89eee531-4eb7-4059-9ad3-99a33d252069  |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
| append      | Boolean     | N        |
| bufferSize  | Int         | N        |
### Rename
This step will rename a file.
#### Steps
| Package                  | Object           | Step     | Id                                    |
|--------------------------|------------------|----------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | rename   | 22c4cc61-1cd8-4ee2-8589-d434d8854c55  |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
| destPath    | String      | Y        |
### Get Size
This step will return the size of the specified file in bytes.
#### Steps
| Package                  | Object           | Step      | Id                                    |
|--------------------------|------------------|-----------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getSize   | b38f857b-aa37-440a-8824-659fae60a0df  |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
### Exists
This step will determine whether a file exists.
#### Steps
| Package                  | Object           | Step     | Id                                    |
|--------------------------|------------------|----------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | exists   | aec5ebf7-7dac-4132-8d58-3a06b4772f79  |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
### Get File Listing
This step will return a list of files.
#### Steps
| Package                  | Object           | Step           | Id                                   |
|--------------------------|------------------|----------------|--------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getFileListing | 71ff49ef-1256-415f-b5d6-06aaf2f5dde1 |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
| recursive   | Boolean     | N        |
### Get Directory Listing
This step will return a list of directories.
#### Steps
| Package                  | Object           | Step                | Id                                    |
|--------------------------|------------------|---------------------|---------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | getDirectoryListing | c941f117-85a1-4793-9c4d-fdd986797979  |
#### Parameters
| Name        | Type        | Required |
|-------------|-------------|----------|
| fileManager | FileManager | Y        |
| path        | String      | Y        |
### Read Header
This step will load the first line of a file and parse it into a Schema.
#### Steps
| Package                  | Object           | Step       | Id                                   |
|--------------------------|------------------|------------|--------------------------------------|
| com.acxiom.metalus.steps | FileManagerSteps | readHeader | 100b2c7d-c1fb-5fe2-b9d1-dd9fff103272 |
#### Parameters
| Name     | Type              | Required |
|----------|-------------------|----------|
| file     | FileResource      | Y        |
| options  | DataStreamOptions | Y        |
## Data Connectors
Data connectors are designed to work with data that lives in a store such as an RDBMS, document or other data system. Data
connectors provide the _createDataReference_ which will return a [Data Reference](data-references.md).
## Stream Connectors
Stream connectors are designed to work with streaming data.
## Stream Steps
### Read Stream
This step is provided as a way to stream data from a source as windows and perform operations. The data will be loaded
from the _Connector_ using the _DataRowReader_ and converted into a [Data Reference](data-references.md) which can then
execute SQL. The retry policy allows some control over the handling of failures. An optional _DataStreamOptions_ will
be passed to the connector when building the reader to control the window size. See the specific connector for more
information on available options.
#### Steps
| Package                  | Object         | Step       | Id                                    |
|--------------------------|----------------|------------|---------------------------------------|
| com.acxiom.metalus.steps | FlowUtilsSteps | streamData | ef1028ad-8cc4-4e20-b29d-d5d8e506dbc7  |
#### Parameters
| Name         | Type              | Required |
|--------------|-------------------|----------|
| source       | Connector         | Y        |
| pipelineId   | String            | Y        |
| retryPolicy  | RetryPolicy       | N        |
| options      | DataStreamOptions | N        |
## Connector Steps
Connector steps are provided to assist loading connectors using a URI.
### Steps
| Package                  | Object         | Step             | Id                                   |
|--------------------------|----------------|------------------|--------------------------------------|
| com.acxiom.metalus.steps | ConnectorSteps | getConnector     | 352abe61-6852-4844-9435-2ca427bcd45b |
| com.acxiom.metalus.steps | ConnectorSteps | getFileConnector | c4c03542-e70b-4b3d-ac6a-ce6065a4fbf7 |
| com.acxiom.metalus.steps | ConnectorSteps | getDataConnector | 543ee9e3-0a73-49d9-993b-5531b3e4ef1b |
### Parameters
| Name         | Type              | Required |
|--------------|-------------------|----------|
| name         | String            | Y        |
| uri          | String            | Y        |
| parameters   | Map               | N        |

## Example Configurations
It is recommended that connectors be defined in the globals of an application and use credential names and not the
credential object unless it uses mappings for sensitive information.
### Local Connector
```json
{
  "localConnector": {
    "className": "com.acxiom.metalus.connectors.LocalFileConnector",
    "object": {
      "name": "my-connector"
    }
  }
}
```
### SFTP Connector
```json
{
  "sftpConnector": {
    "className": "com.acxiom.metalus.connectors.SFTPFileConnector",
    "object": {
      "name": "my-connector",
      "hostName": "sftp.myhost.com",
      "port": 24,
      "credentialName": "!commandLineName"
    }
  }
}
```
### JDBC DataConnector
```json
{
  "sftpConnector": {
    "className": "com.acxiom.metalus.connectors.jdbc.JDBCDataConnector",
    "object": {
      "name": "my-connector",
      "url": "jdbc:derby:memory:!{dbName};create=true",
      "credentialName": "!commandLineName"
    }
  }
}
```
