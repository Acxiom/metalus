[Home](../readme.md)

# Pipelines
A pipeline defines a workflow by combining executable [FlowSteps](steps.md#flowstep). Pipelines may be executed as a single
execution or be embedded in other Pipelines in the form of a [PipelineStepGroup](steps.md#step-group).

* [Pipeline State Key](#pipeline-state-key)
* [Pipeline Sections](#pipeline-sections)
* [Running](#running)

## Pipeline State Key
A universal key allows every pipeline, step, step response, pipeline parameter and audit can be uniquely be identified.
These keys are specific enough that a specific instance of a step running in a fork may be identified. This universal key
facilitates accurately recovering failed processes, restarting steps, mapping flowstep parameter values and providing
detailed audits.

It is important to understand how the keys are constructed in order to more accurately use them as references. All keys
begin with the _root_ pipeline id. This is usually the id of the [application root pipeline id](application.md#root-pipeline).
This is enough when constructing a key to access [pipeline parameters](application.md#pipeline-parameters) and some audits.

Pipeline Key:
```
rootpipeline
```
Pipeline Step Key:
```
rootPipeline.step1
```
Pipeline Step Group Key:
```
rootPipeline.stepGroup.subPipeline.step
```
Pipeline Fork Step Key:
```
rootPipeline.stepGroup.subPipeline.step.f(0)
```

**Note:** _When performing step parameter mappings locally to a pipeline, it is enough to use the parameter or step name
as a reference and the engine will handle constructing the full key._

## Pipeline Sections
### Id
The _id_ is required and is used heavily in the creation of [PipelineStateKeys](). When a Pipeline is defined in a JSON
file, the name of the file should match the id.
### Name
The _name_ of the pipeline. This should be something worth displaying in logs or a UI to make it easier to identify what
this pipeline does.
### Steps
The _steps_ array contains steps that may be executed. During execution, the first step in the array will be executed and
then the flow will based on the _nextSteps_ (or the deprecated _nextStepId_) attribute. The only exception are
[branch steps](steps.md#branch) which have different flow controls.
### Tags
The _tags_ array contains any tags that may be used to categorize this pipeline. This array is not used during execution,
### Description
The _description_ attribute exists to contain a larger description of what this pipeline does.
### Parameters
The _parameters_ object specifies the inputs, output and restartable steps for this pipeline. 
#### Inputs
Provides an array of input parameters which this pipeline expects and whether they are required. Additionally, the
parameter can specify whether the parameter should be on the _globals_ or the _pipeline parameters_. Below is an example
input parameter that specifies this pipeline _requires_ a parameter named **step2Value** to be present on the globals.

```json
{
  "inputs": [
    {
      "name": "step2Value",
      "global": true,
      "required": true
    }
  ]
}
```
#### Output
This attribute specifies the output of this pipeline execution. When embedding pipelines, this provides a way to produce 
a single result for the _step group_. The final output will be a _PipelineStepResponse_ which provides a primary and
secondary results. The _primaryMapping_ takes a single [mapping expression](flow-step-parameter-mapping.md) to fetch the
final value. The _secondaryMappings_ takes a list of named mappings which will produce a map of results keyed by the name
and applying the mapping statement to get a final result. Below is an example output which provides a primary result that
evaluates to the primary response of _STEP_3_ and a secondary response which maps the primary response of _STEP_2_ to 
a key with the name of _steep2Result_.

```json
{
  "output": {
    "primaryMapping": "@STEP_3",
    "secondaryMappings": [
      {
        "mappedName": "step2Result",
        "resultMapping": "@STEP_2"
      }
    ]
  }
}
```
#### Restartable Steps
The _restartableSteps_ array list the flow steps which can be used to restart this pipeline. This mechanism allows control
to specify where in a pipeline the system can perform a restart or recovery cleanly. Some steps will not behave properly
if restarted. A good example is a step that operates on data in memory. Unless the data is already in memory, it makes
sense to force the restart to occur in the steps that loads the data. Below is an example that list several steps that
may be used to restart:

```json
{
  "restartableSteps": [
    "STEP_1",
    "STEP_2",
    "STEP_4"
  ]
}
```
## Running
The easiest way to run a pipeline is through an application. This will handle configuring the required *PipelineContext*
prior to execution. However, it might be beneficial to execute pipelines directly from code. The below example provides
enough code to setup and execute a pipeline that exists on the classpath. **Note**: Exception handling is not provided.

```scala
object ExamplePipelineRunner {
  def main(args: Array[String]): Unit = {
    // Extract command line parameters
    val parameters = DriverUtils.extractParameters(args, None)
    // Build a default ContextManager
    val contextManager: ContextManager = new ContextManager(Map(), Map())
    // Build a generic PipelineContext
    val pipelineContext = PipelineContext(Some(parameters),
      List[PipelineParameter](),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      }
      else {
        List("com.acxiom.metalus", "com.acxiom.metalus.steps")
      }),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      List(), PipelineManager(List()), None, contextManager)
    // Load the Pipeline from a JSON file on the classpath named: my-pipeline-id.json
    val pipeline = pipelineContext.pipelineManager.getPipeline("my-pipeline-id").get.head
    // Execute the pipeline and get the results
    val results = PipelineExecutor.executePipelines(pipeline, pipelineContext)
    // See if the execution was a success
    println(results.success)
  }
}
```
