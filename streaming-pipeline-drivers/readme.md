# Streaming Pipeline Drivers
This project adds additional drivers that read data from streaming technologies using Spark Streaming API. As data is 
consumed, it is converted to a DataFrame and the pipelines are executed to process the data. Application developers
will need to create a step that processes the DataFrame to perform additional conversions that *may* be required before
processing with existing steps.

Visit [spark-pipeline-engine](../spark-pipeline-engine/readme.md) for more information on how drivers work.

## DriverSetup
In addition to the basic DriverSetup functions mentioned in the Spark Pipeline Engine, streaming applications should
override the *refreshExecutionPlan* function. This function will be called prior to invoking the execution plan and 
gives the application a chance to reset any values in the context prior to processing data.

## KafkaPipelineDriver
This driver provides basic support for streaming data from [Kafka](http://kafka.apache.org/) topics. As data is consumed,
the RDD will be converted into a DataFrame with three columns:

* **key** - the record key
* **value** - the data
* **topic** - The topic the data arrived on

### Command line parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **topics** - a comma separated list of topics to monitor
* **kafkaNodes** - a comma separated list of Kafka brokers to consume data

*Optional Parameters:*
* **duration-type** - [minutes | **seconds**] Corresponds to the *duration* parameter.
* **duration** - [number] How long the driver should wait before processing the next batch of data. Default is 10 seconds.
* **groupId** - [string] This is the group id where the Kafka consumer should listen
* **terminationPeriod** - [number] The number of ms the system should run and then shut down.

## KinesisPipelineDriver
This driver provides basic support for streaming data from [Kinesis](https://aws.amazon.com/kinesis/) streams. As data
is consumed, the RDD will be converted into a DataFrame with three columns:

* **key** - the partitionKey
* **value** - the data
* **topic** - The appName

### Command line Parameters
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
  