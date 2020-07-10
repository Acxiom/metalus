[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# KinesisSteps
KinesisSteps provides steps that allow writing a DataFrame to a Kinesis Stream.

## Write to Path
This function will write a given DataFrame to the provided path. Full parameter descriptions are listed below:

* **dataFrame** - A dataFrame to be written to S3.
* **region** - A valid AWS region
* **streamName** - The Kinesis stream where data will be pulled

*Optional Parameters:*
* **separator** - An optional separator character to use when formatting rows into a message
* **accessKeyId** - The AWS access key used to connect
* **secretAccessKey** - The AWS access secret used to connect
