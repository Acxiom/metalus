[Documentation Home](../../docs/readme.md) | [Kafka Home](../readme.md)

# KafkaPipelineDriver
This driver provides basic support for streaming data from [Kafka](http://kafka.apache.org/) topics. As data is consumed,
the RDD will be converted into a DataFrame with three columns:

* **key** - the record key
* **value** - the data
* **topic** - The topic the data arrived on

## Command line parameters
*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
* **topics** - a comma separated list of topics to monitor
* **kafkaNodes** - a comma separated list of Kafka brokers to consume data

*Optional Parameters:*
* **duration-type** - [minutes | **seconds**] Corresponds to the *duration* parameter.
* **duration** - [number] How long the driver should wait before processing the next batch of data. Default is 10 seconds.
* **groupId** - [string] This is the group id where the Kafka consumer should listen
* **terminationPeriod** - [number] The number of ms the system should run and then shut down.
