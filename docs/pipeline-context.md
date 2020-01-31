[Documentation Home](readme.md)

# Pipeline Context
When a [pipeline](pipelines.md) is executed state is maintained using an object named _pipelineContext_ which is an 
implementation of the _PipelineContext_ class. Each [step](pipeline-steps.md) that executed may access this context
simply by adding a parameter to the step function. The presence of this parameter informs Metalus that the step would
like the context injected.

The _PipelineContext_ contains several fields that may be used by a step to access the current state of the pipeline
execution:

* **sparkConf** - The Spark configuration. 
* **sparkSession** - The current Spark session that the application is executing within.
* **globals** - Values that are available within the actual execution running thi current pipeline and step.
* **security** - A _PipelineSecurityManager_ that is used to secure values that are being mapped.
* **parameters** - The pipeline parameters being used. Contains initial parameters as well as the result of steps that 
have been processed.
* **stepPackages** - A list of packages where Metalus will look for steps being executed.
* **parameterMapper** - The _PipelineStepMapper_ being used to map parameters to pipeline steps.
* **pipelineListener** - The _PipelineListener_ registered with the execution.
* **stepMessages** - A Spark accumulator used for registering message for code that executes remotely.
* **rootAudit** - The base [ExecutionAudit](executionaudits.md) used for tracking pipeline and step audits.
* **pipelineManager** - The _PipelineManager_ used to load pipelines during the exection and for [step groups](step-groups.md).
