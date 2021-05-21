[Documentation Home](readme.md)

# Flow Control
Metlaus offers several ways to control the flow of a pipeline:

## Execute If Empty
[Pipeline steps](pipeline-steps.md) have a special attribute named _executeIfEmpty_ which will only execute a step if
the value being evaluated is empty. The value may be static or [mapped](parameter-mapping.md) like step parameters.

This feature is useful for optimizing applications that use chained pipelines which could benefit from sharing resources.
Consider an application that runs two pipelines, during the execution of the first pipeline a DataFrame is created that 
reads from a parquet table and performs some operations. The second pipeline also needs to read data from the parquet 
table. However, since the second pipeline may be restarted without the first pipeline being executed, it will need a step 
that reads the data from the parquet table. By passing the DataFrame from the first pipeline into the *executeIfEmpty* 
attribute, the step will only be executed if the the DataFrame is missing. This allows sharing the DAG across pipelines 
which will also allow Spark to perform optimizations.

## Branch
The branch step type provides a **decision** point in the flow of a pipeline. This step adds a new parameter type of 
**result** that is used to determine the **nextStepId**. The logic in the step function must return a value that may be 
matched to one of the parameters by name. As an example, if there are three *result* type parameters defined 
("One", "Two", "Three"), then the output of the branch step, must be either "One", "Two" or "Three" in order to keep 
processing. In the case there is no match, then processing for that pipeline will stop normally. When defining this type 
of step, the type should be "branch".

## [Step Group](step-groups.md)
The step-group type step provides a mechanism for embedding pipelines within another pipeline. Additional information
may be found [here](step-groups.md).

## [Fork/Join](fork-join.md)
The fork and join step types are simple constructs that allow processing a list of data in a loop like construct and then 
joining the results back for further processing. More information may be found [here](fork-join.md).

## [Split/Merge](split-merge.md)
The split and merge step types allow pipeline designers the ability to run different step sequences in parallel. The 
merge step is used to indicate where the executions should stop and normal processing resume. More information may be
found [here](split-merge.md).

## Stop Execution
There are two ways to stop pipelines:

* **PipelineStepMessage** - A step may register a message and set the type of either *pause* or *error* that will prevent
additional pipelines from executing. The current pipeline will complete execution.
* **PipelineStepException** - A step may throw an exception based on the *PipelineStepException* which will stop the
execution of the current pipeline.

### Exceptions
Throwing an exception that is not a *PipelineStepException* will result in the application stopping and possibly being
restart.
