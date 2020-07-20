[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# KinesisSteps
KinesisSteps provides steps that allow writing a DataFrame to a Kinesis Stream.

## Write to Stream
This function will write a given DataFrame to the provided stream. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **region** - A valid AWS region
* **streamName** - The Kinesis stream where data will be written
* **partitionKey** - The key to use when partitioning the message 

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **accessKeyId** - The AWS access key used to connect
* **secretAccessKey** - The AWS access secret used to connect

## Write to Stream Using Global Credentials
This function will write a given DataFrame to the provided stream. This version will attempt to pull credentials from the
CredentialProvider in the PipelineContext. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **region** - A valid AWS region
* **streamName** - The Kinesis stream where data will be written
* **partitionKey** - The key to use when partitioning the message 

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message

## Post a Message
This function will wWrite a single message to a Kinesis Stream. This version will attempt to pull credentials from the
CredentialProvider in the PipelineContext. Full parameter descriptions are listed below:

* **message** - The message to post.
* **region** - A valid AWS region
* **streamName** - The Kinesis stream where data will be written
* **partitionKey** - The key to use when partitioning the message 

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message

## Post a Message
This function will write a given message to the provided stream. Full parameter descriptions are listed below:

* **message** - The message to post.
* **region** - A valid AWS region
* **streamName** - The Kinesis stream where data will be written
* **partitionKey** - The key to use when partitioning the message 

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **accessKeyId** - The AWS access key used to connect
* **secretAccessKey** - The AWS access secret used to connect
