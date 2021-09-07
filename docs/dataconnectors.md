[Documentation Home](readme.md)

# Data Connectors
Data Connectors provide an abstraction for loading and writing data. This is useful for creating generic pipelines that
can used across providers without source/destination knowledge prior to runtime. Each connector has the responsibility
to load and write a DataFrame based on the underlying system. Below is a breakdown of how connectors may be classified:

![DataConnectors](images/DataConnectors.png)

## Batch
Connectors that are designed to load and write data for batch processing will extend the _BatchDataConnector_. These
are very straightforward and offer the most reusable components.

### HDFSDataConnector
This connector provides access to HDFS. The _credentialName_ and _credential_ parameters are not used in this implementation,
instead relying on the permissions of the cluster. Below is an example setup:

#### Scala
```scala
val connector = HDFSDataConnector("my-connector", None, None,
        DataFrameReaderOptions(format = "csv"),
        DataFrameWriterOptions(format = "csv", options = Some(Map("delimiter" -> "þ"))))
```
#### Globals JSON
```json
{
  "connector": {
    "className": "com.acxiom.pipeline.connectors.HDFSDataConnector",
    "object": {
      "name": "my-connector",
      "readOptions": {
        "format": "csv"
      },
      "writeOptions": {
        "format": "csv",
        "options": {
          "delimiter": "þ"
        }
      }
    }
  }
}
```
### S3DataConnector
This connector provides access to S3. Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = S3DataConnector("my-connector", Some("my-credential-name-for-secrets-manager"), None,
        DataFrameReaderOptions(format = "csv"),
        DataFrameWriterOptions(format = "csv", options = Some(Map("delimiter" -> "þ"))))
```
#### Globals JSON
```json
{
  "connector": {
    "className": "com.acxiom.aws.pipeline.connectors.S3DataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "readOptions": {
        "format": "csv"
      },
      "writeOptions": {
        "format": "csv",
        "options": {
          "delimiter": "þ"
        }
      }
    }
  }
}
```
### GCSDataConnector
This connector provides access to GCS. Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = GCSDataConnector("my-connector", Some("my-credential-name-for-secrets-manager"), None,
        DataFrameReaderOptions(format = "csv"),
        DataFrameWriterOptions(format = "csv", options = Some(Map("delimiter" -> "þ"))))
```
#### Globals JSON
```json
{
  "connector": {
    "className": "com.acxiom.gcp.pipeline.connectors.GCSDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "readOptions": {
        "format": "csv"
      },
      "writeOptions": {
        "format": "csv",
        "options": {
          "delimiter": "þ"
        }
      }
    }
  }
}
```
## Streaming (Coming Soon)
Streaming connectors offer a way to use pipelines with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) without 
the need to write new [drivers](pipeline-drivers.md). When designing pipelines for streaming, care must be taken to not
inject steps that are more batch oriented such as doing a file copy.
