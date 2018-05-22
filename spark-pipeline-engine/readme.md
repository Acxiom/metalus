# Spark Pipeline Engine
This component provides a framework for building Spark processing pipelines built with reusable steps.

## High Level Class Overview

### Pipeline
The Pipeline case class describes the pipeline that needs to be executed. Only three attributes are required:

* **id** - a unique id to identify the pipeline.
* **name** - A name to display in logs and error messages.
* **steps** - A list of steps to execute.

![Default Pipeline Execution](../docs/images/Default_Pipeline_Execution.png "Default Pipeline Execution")

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

### PipelineStep
The PipelineStep describes the step functions that need to be called including how data is passed between steps. When 
creating a PipelineStep, these values need to be populated:

* **id** - This is the step id. This should be unique for the step within the pipeline. This id is how other steps can reference the result of executing the step or how to indicate which step to execute next in the pipeline.
* **displayName** - This is used for logging purposes.
* **description** - This is used to explain what the step is for. When step metadata is stored as another format such as JSON or XML, the description is useful for display in a UI.
* **type** - This describes the type of step and is useful for categorization. Most steps should default to *Pipeline*. There is a *branch* type that is used for branching the pipeline conditionally.
* **params** - This is a list of **Parameter** objects that describe how to populate the step function parameters. 
* **engineMeta** - This contains the name of the object and function to execute for this step. The format is *StepObject.StepFunction*. Note that parameters are not supplied as the *params* attribute handles that.
* **nextStepId** - Tells the system which step to execute next. *branch* steps handle this differently and passing in *None* tells the system to stop executing this pipeline.
* **executeIfEmpty** - This field is used to determine if the current step should execute. Passing in a value skips execution and returns the value as the step response. Fields can use any values that are supported by *params*.

#### PipelineStep Values Special Syntax
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

##### PipelineContext
The *PipelineContext* is a shared object that contains the current state of the pipeline execution. This includes all
global values as well as the result from previous step executions for all pipelines that have been executed.

**Note** that if a step function has *pipelineContext: PipelineContext* in the signature it is not required to map the parameter 
as the system will automatically inject the current context.

**Note** that steps only have **read** access to the *PipelineContext* and may not make any changes.

### DefaultPipelineDriver
This driver provides an entry point for the Spark application.

![Default Driver Flow](../docs/images/Default_Driver_Flow.png "Default Driver Flow")

#### Command line parameters
Any application command line parameters will be made available to the DriverSetup class upon initialization.

*Required parameters:*
* **driverSetupClass** - This class will handle all of the initial setup such as building out pipelines, creating the PipelineContext.
 
### DriverSetup
This trait provides the basic function required to process data. Implementations will be required to override three
functions that are used by drivers guide data processing.

### PipelineContext
The PipelineContext is an object that contains current execution information. The context will automatically be passed
into any step function that has a parameter named **pipelineContext**. The context is only available in the driver so
care should be taken to not try and pass the PipelineContext into code that executes remotely. 

#### Global Variables
All global values are stored in the **globals** attribute. This includes all application command line parameters as well
as the current **pipelineId** and **stepId**.

#### Spark Session and Conf
The current SparkSession and SparkConf are made available as attributes.  

#### StepMessages
The PipelineContext provides a Spark Accumulator that may be passed to remote code to allow information to be passed
back to the driver. The accumulator takes **PipelineStepMessage** objects.

