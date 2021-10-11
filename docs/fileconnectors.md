[Documentation Home](readme.md) | [Connectors](connectors.md)

# FileConnectors
File connectors are used to invoke [FileManager](filemanager.md) implementations that can be used with the
[FileManagerSteps](../metalus-common/docs/filemanagersteps.md) object. FileConnectors are used when a pipeline/step
needs to work on a single file without a DataFrame. Most common operations are expected to be the [Copy step](../metalus-common/docs/filemanagersteps.md#copy)
and the [Create FileManager step](../metalus-common/docs/filemanagersteps.md#create-a-filemanager).

**Parameters**
The following parameters are available to all file connectors:

* **name** - The name of the connector
* **credentialName** - The optional credential name to use to authenticate
* **credential** - The optional credential to use to authenticate

## HDFSFileConnector
This connector provides access to the HDFS file system. The _credentialName_ and _credential_ parameters are not used in
this implementation, instead relying on the permissions of the cluster. Below is an example setup:
#### Scala
```scala
val connector = HDFSFileConnector("my-hdfs-connector", None, None)
```
#### Globals JSON
```json
{
  "myHdfsConnector": {
    "className": "com.acxiom.pipeline.connectors.HDFSFileConnector",
    "object": {
      "name": "my-connector"
    }
  }
}
```
## SFTPFileManager
This connector provides access to an SFTP server. In addition to the standard parameters, the following parameters are
available:

* **hostName** - The host name of the SFTP resource
* **port** - The optional SFTP port
* **knownHosts** - The optional file path to the known_hosts file
* **bulkRequests** - The optional number of requests that may be sent at one time
* **config** - Optional config options
* **timeout** - Optional connection timeout

Below is an example setup:
#### Scala
```scala
val connector = SFTPFileConnector("sftp.myhost.com", "my-sftp-connector", None, None)
```
#### Globals JSON
```json
{
  "sftpConnector": {
    "className": "com.acxiom.pipeline.connectors.SFTPFileConnector",
    "object": {
      "name": "my-connector",
      "hostName": "sftp.myhost.com"
    }
  }
}
```
## S3FileManager
This connector provides access to the S3 file system. In addition to the standard parameters, the following parameters are
available:

* **region** - The AWS region
* **bucket** - The S3 bucket

Below is an example setup:
#### Scala
```scala
val connector = S3FileConnector("us-east-1", "my-bucket", "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "connector": {
    "className": "com.acxiom.aws.pipeline.connectors.S3FileConnector",
    "object": {
      "name": "my-connector",
      "region": "us-east-1",
      "bucket": "my-bucket",
      "credentialName": "my-credential-name-for-secrets-manager"
    }
  }
}
```
## GCSFileManager
This connector provides access to the S3 file system. In addition to the standard parameters, the following parameters are
available:

* **projectId** - The project id of the GCS project
* **bucket** - The name of the GCS bucket

Below is an example setup:
#### Scala
```scala
val connector = GCSFileConnector("my-dev-project", "my-bucket", "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "connector": {
    "className": "com.acxiom.gcp.pipeline.connectors.GCSFileConnector",
    "object": {
      "name": "my-connector",
      "projectId": "my-dev-project",
      "bucket": "my-bucket",
      "credentialName": "my-credential-name-for-secrets-manager"
    }
  }
}
```
