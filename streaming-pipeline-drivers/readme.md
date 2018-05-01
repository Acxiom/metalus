# Streaming Pipeline Drivers
This component extends the Spark Pipeline Engine by add drivers that consume data from various technology using the
Spark StreamingContext. As data is consumed, it is converted to a DataFrame and the pipelines are executed to process
the data.

## DriverSetup
In addition to the basic DriverSetup functions mentioned in the Spark Pipeline Engine, streaming applications should
override the *refreshContext* function. This function will be called prior to calling the PipelineExecutor and gives the
application a chance to reset any values in the context prior to processing data.

## KafkaPipelineDriver
This driver provides basic support for streaming data from [Kafka](http://kafka.apache.org/) topics. As data is consumed,
the RDD will be converted into a DataFrame with three columns:

key - the record key
value - the data
topic - The topic the data arrived on

### Command line parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **topics** - a comma separated list of topics to monitor
* **kafkaNodes** - a comma separated list of Kafka brokers to consume data

*Optional Parameters:*
* **duration-type** - should be seconds or minutes
* **duration** - should be a number
* **groupId** - should be a string
* **terminationPeriod** - This is a number (ms) that informs the system to run for the specified amount of time and then shut down.

## KinesisPipelineDriver
This driver provides basic support for streaming data from [Kinesis](https://aws.amazon.com/kinesis/) streams. As data
is consumed, the RDD will be converted into a DataFrame with three columns:

key - the partitionKey
value - the data
topic - The appName

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
* **duration-type** - should be seconds or minutes
* **duration** - should be a number
* **groupId** - should be a string
* **terminationPeriod** - This is a number (ms) that informs the system to run for the specified amount of time and then shut down.
  
  