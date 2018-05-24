# Spark Pipeline Driver
This project aims to make writing [Spark](http://spark.apache.org) applications easier by abstracting the effort to assemble the driver into
reusable steps and pipelines.

## Example
Examples of building pipelines can be found in the [pipeline-drivers-examples](pipeline-drivers-examples/) project

## Concepts
This framework aims to provide all of the tools required to build Spark applications using reusable components allowing
developers to focus on functionality instead of plumbing.

### Drivers
A driver is the entry point of the application. Each driver provides different benefits. The default pipeline driver that
comes with the [spark-pipeline-engine](spark-pipeline-engine/) is useful for single run applications that shutdown after the pipelines have
finished. The [streaming-pipeline-drivers](streaming-pipeline-drivers/) component provides additional drivers that are useful for long running 
applications that connect to a streaming source and continually process incoming data.

![Driver Initialization](docs/images/DefaultPipelineDriver.png "Default Pipeline Driver Flow")

#### Driver Setup
Each provided driver relies on the *DriverSetup* trait. Each project is required to implement this trait and pass the
class name as a command line parameter. The driver will then call the different functions to begin processing data. Visit
[spark-pipeline-engine](spark-pipeline-engine/readme.md) for more information.

#####  SparkConf and SparkSession
This DriverSetup implementation is responsible for initializing the SparkConf and SparkSession. A utility object is
provided named *DriverUtils* that can be used to create the SparkConf.

##### Pipelines
The *pipelines* function is responsible for returning a List of *Pipeline* objects that will be executed.

##### PipelineContext
The *PipelineContext* needs to be initialized and refreshed. Streaming drivers will call the refresh function after each
data set is processed.

### Pipelines
A pipeline is a list of [*PipelineStep*](spark-pipeline-engine/src/main/scala/com/acxiom/pipeline/PipelineStep.scala)s that need to be executed. Each step in a pipeline will be executed until
completion or an exception is thrown.

During execution, the provided *PipelineListener* will receive notifications when the execution starts and finishes, when
a pipeline starts and finishes and when each step starts and finishes. Additionally when an exception is handled, the listener
will be notified.

Visit [spark-pipeline-engine](spark-pipeline-engine/readme.md) for more information.

### Steps
A step is a single reusable code function that can be executed in a pipeline. There are two parts to a step, the actual
function and the *PipelineStep* metadata. The function should define the parameters that are required to execute properly
and the metadata is is used by the pipeline to define how those parameters are populated. The return type may be anything
including *Unit*.

Visit [spark-pipeline-engine](spark-pipeline-engine/readme.md) for more information.

## Building
The project is built using [Apache Maven](http://maven.apache.org/).
To build the project run:

	mvn

(This will clean, build, test and package the jars and generate documentation)

## Running tests
Tests are part of the main build.

## Spark Versions
This project is built using a specific Spark version and Scala version.

TODO: Figure out the spark version or document it.

## Components
The project is made up of three main components:

* Spark Pipeline Engine
* Streaming Pipeline Drivers
* Pipeline Drivers Examples

### Spark Pipeline Engine
This component contains all of the functionality required to build processing pipelines. Processing pipelines contain a
series of PipelineSteps which are executed in order, conditionally or as branching logic. Multiple pipelines may be
chained together in a single Spark session with objects from pipelines being made available to subsequent pipelines.

### Streaming Pipeline Drivers
This component contains drivers classes that connect to various streaming technologies like [Kafka](http://kafka.apache.org/) and [Kinesis](https://aws.amazon.com/kinesis/). Each
class provides a basic implementation that gathers data and then initiates the Spark Pipeline Engine Component for
processing of the incoming data.

### Pipeline Drivers Examples
This component provides examples for using this framework.