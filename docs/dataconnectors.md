[Documentation Home](readme.md) | [Connectors](connectors.md)

# Data Connectors
Data Connectors provide an abstraction for loading and writing data using the [DataConnectorSteps](../metalus-common/docs/dataconnectorsteps.md).
This is useful for creating generic pipelines that can used across providers without source/destination knowledge prior
to runtime. Each connector has the responsibility to load and write a DataFrame based on the underlying system. 

**Parameters**
The following parameters are available to all data connectors:

* **name** - The name of the connector
* **credentialName** - The optional credential name to use to authenticate
* **credential** - The optional credential to use to authenticate


[Batch](./dataconnectors.md#batch)
* [HDFSDataConnector](./dataconnectors.md#hdfsdataconnector)
* [S3DataConnector](./dataconnectors.md#s3dataconnector)
* [GCSDataConnector](./dataconnectors.md#gcsdataconnector)
* [BigQueryDataConnector](./dataconnectors.md#bigquerydataconnector)
* [MongoDataConnector](./dataconnectors.md#mongodataconnector)
* [JDBCDataConnector](./dataconnectors.md#jdbcdataconnector)
* [JSONApiDataConnector](./dataconnectors.md#jsonapidataconnector-experimental)

[Streaming](./dataconnectors.md#streaming)
* [KinesisDataConnector](./dataconnectors.md#kinesisdataconnector)
* [KafkaDataConnector](./dataconnectors.md#kafkadataconnector)

## Batch
Connectors that are designed to load and write data for batch processing will extend the _BatchDataConnector_. These
are very straightforward and offer the most reusable components.

### HDFSDataConnector
This connector provides access to HDFS. The _credentialName_ and _credential_ parameters are not used in this implementation,
instead relying on the permissions of the cluster. Below is an example setup:

#### Scala
```scala
val connector = HDFSDataConnector("my-connector", None, None)
```
#### Globals JSON
```json
{
  "myConnector": {
    "className": "com.acxiom.pipeline.connectors.HDFSDataConnector",
    "object": {
      "name": "my-connector"
    }
  }
}
```
### S3DataConnector
This connector provides access to S3. Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = S3DataConnector("my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "myS3Connector": {
    "className": "com.acxiom.aws.pipeline.connectors.S3DataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager"
    }
  }
}
```
### GCSDataConnector
This connector provides access to GCS. The _source_ parameter of the load function can take multiple paths by providing
a comma separated string. Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = GCSDataConnector("my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "myGCSConnector": {
    "className": "com.acxiom.gcp.pipeline.connectors.GCSDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager"
    }
  }
}
```
### BigQueryDataConnector
This connector provides access to BigQuery. Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = BigQueryDataConnector("temp-bucket-name", "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "bigQueryConnector": {
    "className": "com.acxiom.gcp.pipeline.connectors.BigQueryDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "tempWriteBucket": "temp-bucket-name"
    }
  }
}
```
### MongoDataConnector
This connector provides access to Mongo. Security is handled using the uri or a _UserNameCredential_. In addition to
the standard parameters, the following parameters are available:

* **uri** - The name connection URI

#### Scala
```scala
val connector = MongoDataConnector("mongodb://127.0.0.1/test", "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "customMongoConnector": {
    "className": "com.acxiom.metalus.pipeline.connectors.MongoDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "uri": "mongodb://127.0.0.1/test"
    }
  }
}
```
### JDBCDataConnector
This connector provides access to JDBC. Security is handled using the uri or a _UserNameCredential_. In addition to
the standard parameters, the following parameters are available:

* **url** - The connection URL

#### Scala
```scala
val connector = JDBCDataConnector("jdbc:derby:memory:test", "table_name", "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "customJDBCConnector": {
    "className": "com.acxiom.pipeline.connectors.JDBCDataConnector",
    "object": {
      "name": "my-jdbc-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "url": "jdbc:derby:memory:test"
    }
  }
}
```
### JSONApiDataConnector (Experimental)
This connector provides the ability to interact with data in an API. The ApiHandler trait is used to allow extensibility.
In addition to the standard parameters, the following parameters are available:
* **apiHandler** - The _ApiHandler_ is used to handle parsing/writing the data to/from DataFrames
* **hostUrl** - The url that is hosting data. This doesn't have to include the path since that can be passed in the _load/write_ functions
* **authorizationClass** - This is the fully qualified class name to use when authenticating to the API
* **allowSelfSignedCertificates** - Flag indicating whether self-signed certificates should be accepted from the API
#### ApiHandler
The _ApiHandler_ is used to handle parsing/writing the data to/from DataFrames. There are two ways to parse data from the JSON,
as a list of maps and as a list of list.
#### Scala
```scala
val connector = JSONApiDataConnector(apiHandler, "my-connector", Some("my-credential-name-for-secrets-manager"), None)
```
#### Globals JSON
```json
{
  "jsonApiConnector": {
    "className": "com.acxiom.pipeline.connectors.JSONApiDataConnector",
    "object": {
      "apiHandler": {
        "jsonDocumentPath": "location.within.json.to.fetch.rows",
        "useRowArray": true,
        "hostUrl": "https://localhost:8080/",
        "authorizationClass": "com.acxiom.pipeline.api.BasicAuthorization",
        "allowSelfSignedCertificates": true,
        "schema": {
          "attributes": [
            {
              "name": "column1",
              "dataType": {
                "baseType": "string"
              }
            },
            {
              "name": "column2",
              "dataType": {
                "baseType": "integer"
              }
            },
            {
              "name": "column3",
              "dataType": {
                "baseType": "string"
              }
            }
          ]
        }
      },
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager"
    }
  }
}
```
## Streaming
Streaming connectors offer a way to use pipelines with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) without 
the need to write new [drivers](pipeline-drivers.md). When designing pipelines for streaming, care must be taken to not
inject steps that are more batch oriented such as doing a file copy. When using streaming connectors, the
[monitor step](../metalus-common/docs/flowutilssteps.md#streaming-monitor) should be used and the command line parameter
**streaming-job** should be set to true when invoking the [Default Pipeline Driver](pipeline-drivers.md#default-pipeline-driver).

### KinesisDataConnector
This connector provides access to Kinesis. In addition to the standard parameters, the following parameters are
available:

* **streamName** - The name of the Kinesis stream.
* **region** - The region containing the Kinesis stream
* **partitionKey** - The optional static partition key to use
* **partitionKeyIndex** - The optional field index in the DataFrame row containing the value to use as the partition key
* **separator** - The field separator to use when formatting the row data
* **initialPosition** - The starting point to begin reading data (trim_horizon, latest, at_timestamp) from the shard.
                        *trim_horizon* and _latest_ can be passed as strings. *at_timestamp* needs to be a json object.
                        An example is provided below. Default is *trim_horizon*

Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = KinesisDataConnector("stream-name", "us-east-1", None, Some(15), ",", 
  "{\"at_timestamp\": \"06/25/2020 10:23:45 PDT\", \"format\": \"MM/dd/yyyy HH:mm:ss ZZZ\"}", "my-connector",
  Some("my-credential-name-for-secrets-manager"))
```
#### Globals JSON
```json
{
  "kinesisConnector": {
    "className": "com.acxiom.aws.pipeline.connectors.KinesisDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "streamName": "stream-name",
      "region": "us-east-1",
      "separator": ",",
      "initialPosition": "{\"at_timestamp\": \"06/25/2020 10:23:45 PDT\", \"format\": \"MM/dd/yyyy HH:mm:ss ZZZ\"}"
    }
  }
}
```
### KafkaDataConnector
This connector provides access to Kinesis. In addition to the standard parameters, the following parameters are
available:

* **topics** - The name of the Kinesis stream.
* **kafkaNodes** - The region containing the Kinesis stream
* **key** - The optional static key to use
* **keyField** - The optional field name in the DataFrame row containing the value to use as the key
* **separator** - The field separator to use when formatting the row data
* 
Below is an example setup that expects a secrets manager credential provider:
#### Scala
```scala
val connector = KafkaDataConnector("topic-name1,topic-name2", "host1:port1,host2:port2", "message-key", None,
  "my-connector", Some("my-credential-name-for-secrets-manager"))
```
#### Globals JSON
```json
{
  "kafkaConnector": {
    "className": "com.acxiom.kafka.pipeline.connectors.KafkaDataConnector",
    "object": {
      "name": "my-connector",
      "credentialName": "my-credential-name-for-secrets-manager",
      "topics": "topic-name1,topic-name2",
      "kafkaNodes": "host1:port1,host2:port2",
      "key": "message-key"
    }
  }
}
```
