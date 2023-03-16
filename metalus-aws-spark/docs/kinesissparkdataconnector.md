[Documentation Home](../../docs/readme.md) | [AWS Spark Home](../readme.md)

# KinesisSparkDataConnector
Provides a SparkDataConnector implementation that works with Kinesis.
When reading, it will return a SparkDataReference built using a structured streaming dataFrame.
When writing, it will send each of the dataFrame as a message to the given kinesis stream.
Full parameter descriptions are listed below:

* **streamName** - The name of the stream to use.
* **region** - The AWS region to connect through.
* **partitionKey** - The key to use when partitioning the message.
* **partitionKeyIndex** The partition key index.
* **separator** - An optional separator character to use when formatting rows into a message. Default is ",".
* **initialPosition** the initial position for the stream.
