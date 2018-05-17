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
A pipeline is a list of *PipelineStep*s that need to be executed. Each step in a pipeline will be executed until
completion or an exception is thrown. 

#### Pipeline Chaining
Pipelines can be chained together and may be restarted. When restarting a pipeline, all
pipelines after that pipeline will also be executed.

#### Branching
When the flow of the pipeline execution needs to be determined conditionally, a step may be set with a *type* of **branch**.
The return value of the branch should match the name of a parameter in the step params metadata. The value of that parameter
will be used as the id of the next step to execute.

#### Conditional Execution
Each step has an attribute named *executeIfEmpty* which takes a value just like the parameters of the step. If the value
is empty, then the step will be executed. This is useful in pipeline chaining to aid with sharing resources such as a 
DataFrame. 

One example is the application runs two pipelines. During the execution of the first pipeline a DataFrame is created that
reads from a parquet table and performs some operations. The second pipeline also needs to read data from the parquet table.
However, since the second pipeline may be restarted without the first pipeline being executed, it will need a step that
reads the data from the parquet table. By passing the DataFrame from the first pipeline into the *executeIfEmpty* attribute,
the step will only be executed if the the DataFrame is missing. This allows sharing the DAG across pipelines which will
also allow Spark to perform optimizations.

#### Flow Control
There are two ways to stop pipelines:

* **PipelineStepMessage** - A step may register a message and set the type of either *pause* or *error* that will prevent
additional pipelines from executing. The current pipeline will complete execution.
* **PipelineStepException** - A step may throw an exception based on the *PipelineStepException* which will stop the
execution of the current pipeline.

#### Exceptions
Throwing an exception that is not a *PipelineStepException* will result in the application stopping and possibly being
restarted. This should only be done when the error is no longer recoverable.

### Steps
A step is a single reusable code function that can be executed in a pipeline. There are two parts to a step, the actual
function and the *PipelineStep* metadata. The function should define the parameters that are required to execute properly
and the metadata is is used by the pipeline to define how those parameters are populated.

##### PipelineContext
The *PipelineContext* is a shared object that contains the current state of the pipeline execution. This includes all
global values as well as the result from previous step executions for all pipelines that have been executed.

Note that if a step function has *pipelineContext: PipelineContext* in the signature it is not required to map the parameter 
as the system will automatically inject the current context.

#### PipelineStep Metadata Values Special Syntax
PipelineSteps can use special syntax to indicate that a system value should be used instead of a static provided value.

* **!** - When the value begins with this character, the system will search the PipelineContext.globals for the named parameter and pass that value to the step function.
* **@** - When the value begins with this character, the system will search the PipelineContext.parameters for the named parameter and pass the primaryReturn value to the step function.
* **$** - When the value begins with this character, the system will search the PipelineContext.parameters for the named parameter and pass that value to the step function.

In addition to searching the parameters for the current pipeline, the user has the option of specifying a pipelineId in 
the syntax for *@* and *$* to specify any previous pipeline value. *Example: @p1.StepOne*

Values may also be embedded. The user has the option to reference properties embedded in top level objects. Given an 
object (obj) that contains a sub-object (subObj) which contains a name, the user could access the name field using this
syntax:

	$obj.subObj.name
	
Here is the object descried as JSON:

	{
		"subObj": {
			"name": "Spark"
		}
	} 

## Building
The project is built using [Apache Maven](http://maven.apache.org/).
To build the project run:

	mvn

(This will clean, build, test and package the jars and generate documentation)

## Running tests
Tests are part of the main build.

## Spark Versions
This project is built using a specific Spark version and Scala version.

## Components
The project is made up of two main components:

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