[Documentation Home](readme.md)

# Execution Audits
Metalus provides a mechanism for capturing audits and metrics during execution.Three levels are available:

* *execution* - Audits the execution of a single execution. This includes all executed pipelines.
* *pipeline* - Audits a single pipeline being executed.
* *step* - Audits a single step within a pipeline.

The global parameter _logAudits_ will write each execution audits to the log file once execution is completed successfully.

## Metrics
Each audit contains a metrics object where metrics can be set with the **setMetric** and **setMetrics** methods. 

Each audit captures a start and end time for each level as a basic metric. Application developers may inject custom 
metrics within an audit using the registered *PipelineListener*. Below is a detailed explanation of where each level is 
accessible. Steps allow metrics to be added as a secondary return in the *PipelineStepResponse* using the following syntax:

```$metrics.<metric_name>```  

## Audit Levels
### Execution
This audit is automatically started when the "executionStarted" event is triggered and ends when the "executionFinished"
event is fired. The timing should be inclusive of the combined timings of all executed pipelines.

### Pipeline
This audit is automatically started when the "pipelineStarted" event is triggered and ends when the "pipelineFinished" 
event is fired. The timing should be inclusive of the combined timings of all executed steps.

### Step
This audit is automatically started when the "pipelineStepStarted" event is triggered and ends when the "pipelineStepFinished" 
event is fired.

### Fork Steps
All of the steps contained in a fork, including the join, will be tracked based on the *groupId* and rolled up under the 
fork step for analysis. The *groupId* of the step audit can be used to group all steps involved in a single execution.

### Step Groups
All of the audits for a step group pipeline will be children of the step group in the outer pipeline.

## Additional Classes
### PipelineContext
The PipelineContext contains the *rootAudit* which holds all of the data related to the overall execution. Helper 
functions have been provided to make accessing pipeline and step audits easier.

### PipelineListener
Implementations of the PipelineListener interface will have access to the audits through the *PipelineContext*. The *metrics*
functions provided allow any data to be stored/retrieved related to an audit.

## Spark Config/Settings
By default, the Execution Audits will include a small list of spark configuration settings and spark stats by pipeline step.
A more detailed list of SparkSettings from the SparkContext can be added to the Root Audit by setting the value *includeAllSparkSettingsInAudit* to true at runtime.

_**Note:** the current version may misreport executors, split steps, and fork steps running in parallel_
