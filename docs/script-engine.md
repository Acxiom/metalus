[Documentation Home](readme.md)

# Script Engine
The script engine framework provides an extension of the base Metalus functionality. Implementations are provided out of 
the box:
* **Javascript**
* **Scala**

The intent of the script engine is to provide an extension to the Metalus library that doesn't require developers to build
out [step libraries](step-libraries.md) to introduce new functionality. This should be considered a short term solution
to test changes, but it is still a best practice to fold that logic back into a proper step library.

## Steps
In addition to the two engines provided, two steps are provided as part of [metalus-common](../metalus-common/readme.md)
which allow writing custom steps within a pipeline:
* [Javascript Steps](../metalus-common/docs/javascriptsteps.md)
* [Scala Steps](../metalus-common/docs/scalascriptsteps.md)

## Functions
### Execute Simple Script
This function executes a script and returns the result. No other parameters are allowed.
### Execute Script
This function will execute a script, but it makes the [PipelineContext](pipeline-context.md) available as the variable
_pipelineContext_.
### Execute Script with Object
This function will execute a script, but it makes the [PipelineContext](pipeline-context.md) available as the variable
_pipelineContext_ and allows the user to pass an object to the script and made available as _obj_.
