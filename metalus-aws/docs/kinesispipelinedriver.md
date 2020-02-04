[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# KinesisPipelineDriver
This driver provides basic support for streaming data from [Kinesis](https://aws.amazon.com/kinesis/) streams. As data
is consumed, the RDD will be converted into a DataFrame with three columns:

* **key** - the partitionKey
* **value** - the data
* **topic** - The appName

## Command line Parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **appName** - The name of this app to use when check pointing Kinesis sequence numbers.
* **streamName** - The Kinesis stream where data will be pulled
* **endPointURL** - The Kinesis URL where to connect the app.
* **regionName** - A valid AWS region
* **awsAccessKey** - The AWS access key used to connect
* **awsAccessSecret** - The AWS access secret used to connect

*Optional Parameters:*
* **consumerStreams** - [number] The number of streams to create. Defaults to the number of shards defined for the stream.
* **duration-type** - [minutes | **seconds**] Corresponds to the *duration* parameter.
* **duration** - [number] How long the driver should wait before processing the next batch of data. Default is 10 seconds.
* **terminationPeriod** - [number] The number of ms the system should run and then shut down. 
