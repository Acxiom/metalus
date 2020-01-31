[Documentation Home](readme.md)

# Flow Control
Metlaus offers several ways to control the flow of a pipeline:

## Execute If Empty
[Pipeline steps](pipeline-steps.md) have a special attribute named _executeIfEmpty_ which will only execute a step if
the value being evaluated is empty. This is useful when pipelines are being combined in an execution so that resources
may be shared.

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
joining the results back for further processing. More information may be found [here](fork-join.md)

