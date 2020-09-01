[Documentation Home](readme.md)

# Step Templates
A step template is a JSON representation of a step function. There are two ways to get this representation: 1) manually
create the JSON based on the properties of the step function or 2) use the provided [annotations](step-annotations.md) 
when creating the step and run the **metalus-utils** [metadata extractor utility](metadata-extractor.md) to scan step 
jars and produce the JSON.

In order to generate the proper step template, start with a step function:

```scala
  def readFromPath(path: String, options: DataFrameReaderOptions = DataFrameReaderOptions(),
                   pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load(path)
  }
```

This step function is required to exist within a scala **object**. The example step function has three parameters, one of which
is a _PipelineContext_. The *path* and *options* parameters **must** be represented in the step template, but the 
*pipelineContext* parameter is a special library parameter that should **not** be a part of the step template.

## [Metadata Extractor](metadata-extractor.md)
The easiest way to generate step templates is to add an annotation to the step object and step function that may be 
scanned using the [metadata extractor utility](metadata-extractor.md). The **StepFunction** annotation is used to provide 
information that cannot be gathered from the step function as well as mark the function as being a step. The annotation
**must** be fully filled in with the following parameters: id, displayName, description, type, category. A description 
of each is provided in the [JSON Step Template](step-templates.md) section. The scala object containing the step function(s)
must have the **StepObject** annotation. A complete list of annotations may be found [here](step-annotations.md).

```scala
  @StepFunction("87db259d-606e-46eb-b723-82923349640f",
    "Load DataFrame from HDFS path",
    "This step will read a dataFrame from the given HDFS path",
    "Pipeline",
    "InputOutput")
```

The [metadata extractor utility](metadata-extractor.md) will infer the remaining information from the function itself. Tags
will be automatically populated with the name of the jar file where the step function was located.

## Manual
In the absence of annotated code, the step template can be manually created.

## JSON Step Template
Below is a step template based on the example step function. The first five attributes cannot be pulled from the step 
function. Either an annotation must be provided or the step function developer would need to provide the information.

```json
    {
      "id": "87db259d-606e-46eb-b723-82923349640f",
      "displayName": "Load DataFrame from HDFS path",
      "description": "This step will read a dataFrame from the given HDFS path",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "path",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "object",
          "name": "options",
          "required": false,
          "className": "com.acxiom.pipeline.steps.DataFrameReaderOptions",
          "parameterType": "com.acxiom.pipeline.steps.DataFrameReaderOptions"
        }
      ],
      "engineMeta": {
        "spark": "HDFSSteps.readFromPath",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    }
```

### Metadata Attributes
These attributes provide basic information about the step function. The **displayName** attribute may be used during pipeline
execution for logging, but otherwise these attributes are not required for execution.

* **id** - A unique GUID which can be used to identify this step function
* **displayName** - A short name that may be presented to a user in a UI
* **description** - A longer explanation of what this step does
* **type** - The type of step. It can be one of the following values: Pipeline, pipeline, branch, fork, join or step-group
* **category** - Represents a group which this step should belong. This is useful for UIs and queries to provide a common grouping of steps

### Tags
Tags provide an additional layer of grouping within a query or UI. In the example above, the name of the jar file(s) which 
contained the step functions have been added. Tags are free form strings that can be anything.

### Engine Meta
The engine meta section contains the metadata required to execute the step function at runtime. The **spark** attribute
is required and must contain the object name containing the step function and the function name. It should be in the form 
of **<Scala_Object>.<function_name>**. The **pkg** attribute lists the scala package where the step object may be found.
This is not required, but is useful for systems that may use this information to help construct applications at runtime.

#### Results
The results attribute is not required, but is useful in indicating what a step function may return. This information may 
be used in the future as a way to verify that step mappings are compatible. Step function can only return a single value,
but using the **PipelineStepResponse** class, multiple values may be returned. The **primaryType** value will either be
the actual return type or if a PipelineResponse is returned, the developer may choose to provide an additional annotation
which indicates the return type for primary and secondary. The attribute **secondaryTypes** may be added as a JSON object 
containing name value pairs.

### Params
The **params** array contains all of the step function parameters except those of type **PipelineContext**. Each parameter
has seven possible parameters:

* **type** - The parameter [type](parameter-mapping.md#types). Must be one of: integer, list, boolean, script, scalaScript, string, text, result, object
* **name** - The name of the parameter. This **must** match the name of the parameter on the step function.
* **description** - Describes what this parameter should do.
* **required** - Boolean flag indicating whether the requirement of this value
* **defaultValue** - An optional default value that will be used when no value is provided
* **language** - An optional script language this parameter expects the value to conform. This should only be present if the **type** is set to *script*
* **className** - If the **type** is **object**, then this should represent the fully qualified class name that is expected
* **parameterType** - An optional attribute that represents the fully qualified class name (or primitive name) of the 
step function parameter. This may be set regardless of type

#### Branch Steps
Branch steps differ from all other steps in that additional **params** of type **result** may be added which are used to
determine the next step which will be executed. The name of these parameters must match the possible primary return type
value. Only branch steps should have result types and must contain at least one. There is no limit to the number of result
parameters that may be added.

#### Scala Script Parameters
Step parameters given a type of **scalaScript** will be compiled and evaluated, with the result of the script passed to the step.
These scripts consist of a binding section and a scala script. The bindings section must be enclosed by parenthesis.
Each binding contains a name, value, and type, broken out as **\<name>:\<value>:\<type>**. Multiple bindings can be provided using a comma separated list.
Colons can be escaped using a backslash.
A sample scala script is provided below:
```scala
(list:@GetList:Option[List[String]],pattern:!mypattern || [a-z]+:String)
// script
list.getOrElse(List()).filter(s => s.matches(pattern))
```
