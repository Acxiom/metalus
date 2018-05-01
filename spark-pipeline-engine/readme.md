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
The PipelineStep describes the step functions that need to be called including how data is passed between steps.

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

