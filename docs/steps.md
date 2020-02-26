[Documentation Home](readme.md)

# Steps
The first task when preparing an application to use this project is to ensure that the reusable steps are available. A
project containing [common steps](../metalus-common) is available, but most developers will have requirements to 
integrate with existing libraries and APIs. The application developer should start by creating a [project to house these 
steps](step-libraries.md). As new functionality is identified, a new step function/object will need to be created.

Creating a step is a simple process. A scala object is used to house one or more steps as a logical grouping. Public
functions are then created that act as the step. Function parameters should be defined that will be mapped at runtime 
using the [pipeline](pipelines.md) configuration. Care should be taken to ensure that the step function is made as 
generic as possible to ensure re-usability. The complexity of the step function is at the discretion of the application 
developer, but the intent should be to deliver a step that is highly reusable in nature. An additional function parameter 
may be added that does not need to be mapped called [pipelineContext: PipelineContext](pipeline-context.md). This object
will be injected at runtime and contains information about the current pipeline execution.

Finally, the application developer may choose to use [annotations](step-annotations.md) that allow 
[step templates](step-templates.md) to be generated using just the jar file.

## Step Types
The library provides several step types to make building applications easier.

### Pipeline
This is the most common step type used to perform work in the pipeline. When defining this type of step, the type should
be "Pipeline".

### Branch
The branch type step provides a **decision** point in the flow of a pipeline. This step adds a new parameter type of **result**
that is used to determine the **nextStepId**. The logic in the step function must return a value that may be matched to 
one of the parameters by name. As an example, if there are three *result* type parameters defined ("One", "Two", "Three"),
then the output of the branch step, must be either "One", "Two" or "Three" in order to keep processing. In the case there 
is no match, then processing for that pipeline will stop normally. When defining this type of step, the type should
be "branch".

### [Step Group](step-groups.md)
The step-group type step provides a mechanism for embedding pipelines within another pipeline. Additional information
may be found [here](step-groups.md).

### [Fork/Join](fork-join.md)
The fork and join step types are simple constructs that allow processing a list of data in a loop like construct and then 
joining the results back for further processing. Additional information may be found [here](fork-join.md).

## [Advanced Features](advanced-step-features.md)
Additional information pertaining to advanced step features may be found [here](advanced-step-features.md).
