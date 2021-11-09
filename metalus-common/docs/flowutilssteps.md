[Documentation Home](../../docs/readme.md) | [Common Home](../readme.md)

# FlowUtilsSteps
These steps offer various tools for manipulating the flow of a [Pipeline](../../docs/pipelines.md).

## Simple Retry
This step provides a mechanism for determining whether to perform a retry or to stop in the form of a branch step. The
pipeline will need to define a unique name for the counter and the maximum number of retries. The information will be
tracked in the globals. This step is not a replacement for the [step retries](../../docs/pipeline-steps.md#retrylimit)
which provide a mechanism for retrying a single step when an exception occurs.

### Parameters
* **counterName** - The name of the counter to use for tracking.
* **maxRetries** - The maximum number of retries allowed.
### Results
* **retry** - This branch is used to transition to the step that needs to be retried.
* **stop** - This branch is used to call the step when retries have been exhausted.

## Streaming Monitor
This step provides a mechanism to monitor a streaming query in a pipeline. This step can safely be called without 
providing the streaming query and the _stop_ action will be taken. The [StreamingQueryMonitor](streamingquerymonitor.md) framework provides an
integration point for controlling how this step handles the streaming query.

### Parameters
* **query** - The streaming query to monitor.
* **streamingMonitorClassName** - Fully qualified classname of the monitor class. The [default monitor class](streamingquerymonitor.md#basestreamingquerymonitor) never stops the query.
### Results
* **continue** - This branch is used to allow restarting the streaming query.
* **stop** - This branch is used to call the step when streaming should not continue.
