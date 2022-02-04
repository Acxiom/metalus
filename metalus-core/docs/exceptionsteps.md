[Documentation Home](../../docs/readme.md) | [Core Home](../readme.md)

# ExceptionSteps
This step object provides a way to throw exceptions from within a pipeline.

## Throw Pipeline Step Exception
This step will throw the _PipelineException_ which is used to stop execution of the current pipeline.
Full parameter descriptions are listed below:
* **message** - The message to log for this exception.
## Throw Skip Execution Exception
This step will throw the _SkipExecutionPipelineStepException_ which is used in execution evaluation pipelines.
Full parameter descriptions are listed below:
* **message** - The message to log for this exception.
