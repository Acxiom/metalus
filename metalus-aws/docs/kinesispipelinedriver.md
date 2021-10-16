[Documentation Home](../../docs/readme.md) | [AWS Home](../readme.md)

# KinesisPipelineDriver
This driver provides basic support for streaming data from [Kinesis](https://aws.amazon.com/kinesis/) streams. As data
is consumed, the RDD will be converted into a DataFrame with three columns:

* **key** - the partitionKey
* **value** - the data
* **topic** - The appName

The driver will attempt to locate a credential using the optional credential name parameters below. If the parameters
aren't specified, then the defaults are used. Finally the client will be created without using credentials and rely
on the credentials used to start the Spark job.

## Command line Parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **streamName** - The Kinesis stream where data will be pulled
* **region** - A valid AWS region

*Optional Parameters:*
* **appName** - The optional name of this app to use when check pointing Kinesis sequence numbers.
* **kinesisCredentialName** - An optional name of the credential to use when building the Kinesis client. Default AWSCredential
* **kinesisCloudWatchCredentialName** - An optional name of the credential to use for Cloud Watch when building the Kinesis client. Default AWSCloudWatchCredential
* **kinesisDynamoDBCredentialName** - An optional name of the credential to use for DynamoDB when building the Kinesis client. Default AWSDynamoDBCredential
* **accessKeyId** - The AWS access key used to connect
* **secretAccessKey** - The AWS access secret used to connect
* **consumerStreams** - [number] The number of streams to create. Defaults to the number of shards defined for the stream.
* **duration-type** - [minutes | **seconds**] Corresponds to the *duration* parameter.
* **duration** - [number] How long the driver should wait before processing the next batch of data. Default is 10 seconds.
* **terminationPeriod** - [number] The number of ms the system should run and then shut down. 
* **maxRetryAttempts** - [number] The number of times data will attempt to process before failing. Default is 0.
* **terminateAfterFailures** - [boolean] After processing has been retried, fail the process. Default is false.
* **processEmptyRDD** - [boolean] When true, will trigger executions for each window interval
 regardless of whether any messages have been received.
