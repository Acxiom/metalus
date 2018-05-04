# Spark Pipeline Engine
This component provides a framework for building Spark processing pipelines built with reusable steps.

## Example
TODO: Provide a tutorial for building a basic app using the framework and running it against a Spark cluster

## High Level Class Overview

### Pipeline
The Pipeline case class describes the pipeline that needs to be executed. Only three attributes are required:

* **id** - a unique id to identify the pipeline.
* **name** - A name to display in logs and error messages.
* **steps** - A list of steps to execute.

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
Note that if a function has *pipelineContext: PipelineContext* in the signature it is not required to map the parameter 
as the system will automatically inject the current context.

### DefaultPipelineDriver
This driver provides an entry point for the Spark application.

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

