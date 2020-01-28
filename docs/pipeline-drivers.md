# [Documentation Home](readme.md)

# Pipeline Drivers
Pipeline drivers are the entry point for any Metalus application. A default and Kafka based (streaming) drivers are 
provided. The pipeline driver chosen will use a *DriverSetup* to configure the application prior to execution.

## DriverSetup
The *DriverSetup* is invoked by the chosen driver class with a map containing all of the application command line 
parameters from the 'spark-submit' command. The *DriverSetup* will then be responsible for creating the *SparkSession*, 
*PipelineContext* and execution plan. When executing the 'spark-submit' class, one application parameter is required, 
*driverSetupClass* which is used to initialize the *DriverSetup* implementation.

![Driver Initialization](images/DefaultPipelineDriver.png "Default Pipeline Driver Flow")

This flow demonstrates how the chosen driver interacts with the *DriverSetup*:

![Default Driver Flow](images/Default_Driver_Flow.png "Default Driver Flow")

There are no special instructions for creating the *SparkSession*. Both the *SparkSession* and *SparkConf* are required
by the *PipelineContext*.

The *PipelineContext* is a shared object that contains the current state of the pipeline execution. This includes all
global values as well as the result from previous step executions for all pipelines that have been executed. In addtiona
to the *SparkSession* and *SparkConf* here are the additional parameters:

* **Global Parameters** - It is recommended that the parameters passed to the *DriverSetup* be used as the initial globals map
* **Security Manager** - Unless the application needs additional security, use the *PipelineSecurityManager* class.
* **Pipeline Parameters** - This object stores the output of executed steps. Basic initialization is: *PipelineParameters(List())* 
* **Step Packages** - This is a list of package names the framework should scan when executing steps. At a minimum it 
should contain *com.acxiom.pipeline.steps*
* **Pipeline Step Mapper** - This object is responsible for pipeline mapping. The default is PipelineStepMapper().  
* **Pipeline Listener** - This object provides a way to track progress within the pipelines. The default is 
instantiation is *DefaultPipelineListener()*. It is recommended that application developers create an implementation
specific to the application.
* **Step Messages** - This is a Spark collection accumulator of *PipelineStepMessage* objects that allow remote executions
to communicate back to the driver. Basic initialization should be 
*```sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")```*.
* **Root Audit** - This is the root [audit](executionaudits.md) for the execution.
* **Pipeline Manager** - This class manages access to pipelines that may be used by step groups during an execution.

![Driver Initialization](images/DefaultPipelineDriver.png "Default Pipeline Driver Flow")

## DriverSetup - Streaming
Using Spark Streaming API, additional drivers provide streaming functionality. As data is consumed, it is converted to a 
DataFrame and the pipelines are executed to process the data. Application developers will need to create a step that 
processes the DataFrame to perform additional conversions that *may* be required before processing with existing steps.

In addition to the basic DriverSetup functions mentioned in the Metalus Pipeline Core, streaming applications should
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
