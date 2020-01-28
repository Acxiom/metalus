# [Documentation Home](readme.md)

# JSON Pipelines
Building pipelines in JSON provide developers a way to construct Spark applications using existing **step libraries** without
the need to write and deploy code. Once a pipeline is designed it may be delivered several ways:

* As part of an application JSON
* An API (a custom PipelineManager or DriverSetup would be required)
* Embedded in a jar
* A disk (local/hdfs/s3, etc...) location (a custom PipelineManager or DriverSetup would be required)

The **Metalus** library will then convert the JSON into a scala object to be executed.

There are three components needed to create a pipeline:

* Step Template - This is the JSON that describes the step. Pipeline Steps start with this template and modify to satisfy pipeline requirements.
* Pipeline Step - The step template is copied and modified with additional attributes and mappings before being added to the pipeline JSON.
* Pipeline - Contains information about the pipeline including which steps to execute at runtime.

## Step Templates
A step template is a JSON representation of a step function. There are two ways to get this representation: 1) manually
create the JSON based on the properties of the step function or 2) use the provided annotations when creating the step 
and run the **metalus-utils** [metadata extractor utility](metadata-extractor.md) to scan step jars and produce the JSON.

In order to generate the proper step template, start with a step function:

```scala
  def readFromPath(path: String, options: DataFrameReaderOptions = DataFrameReaderOptions(),
                   pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load(path)
  }
```

This step function is required to exist within a scala **object**. The example step function has three parameters, one of which
is a **PipelineContext**. The *path* and *options* parameters **must** be represented in the step template, but the 
*pipelineContext* parameter is a special library parameter that should **not** be a part of the step template.

### Metadata Extractor
The easiest way to generate step templates is to add an annotation to the step object and step function that may be 
scanned using the [metadata extractor utility](metadata-extractor.md). The **StepFunction** annotation is used to provide 
information that cannot be gathered from the step function as well as mark the function as being a step. The annotation
**must** be fully filled in with the following parameters: id, displayName, description, type, category. A description 
of each is provided in the [JSON Step Template](#json-step-template) section. The scala object containing the step function(s)
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

### Manual
In the absence of annotated code, the step template can be manually created.

### JSON Step Template
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

#### Metadata Attributes
These attributes provide basic information about the step function. The **displayName** attribute may be used during pipeline
execution for logging, but otherwise these attributes are not required for execution.

* **id** - A unique GUID which can be used to identify this step function
* **displayName** - A short name that may be presented to a user in a UI
* **description** - A longer explanation of what this step does
* **type** - The type of step. It can be one of the following values: Pipeline, pipeline, branch, fork, join or step-group
* **category** - Represents a group which this step should belong. This is useful for UIs and queries to provide a common grouping of steps

#### Tags
Tags provide an additional layer of grouping within a query or UI. In the example above, the name of the jar file(s) which 
contained the step functions have been added. Tags are free form strings that can be anything.

#### Engine Meta
The engine meta section contains the metadata required to execute the step function at runtime. The **spark** attribute
is required and must contain the object name containing the step function and the function name. It should be in the form 
of **<Scala_Object>.<function_name>**. The **pkg** attribute lists the scala package where the step object may be found.
This is not required, but is useful for systems that may use this information to help construct applications at runtime.

##### Results
The results attribute is not required, but is useful in indicating what a step function may return. This information may 
be used in the future as a way to verify that step mappings are compatible. Step function can only return a single value,
but using the **PipelineStepResponse** class, multiple values may be returned. The **primaryType** value will either be
the actual return type or if a PipelineResponse is returned, the developer may choose to provide an additional annotation
which indicates the return type for primary and secondary. The attribute **secondaryTypes** may be added as a JSON object 
containing name value pairs.

#### Params
The **params** array contains all of the step function parameters except those of type **PipelineContext**. Each parameter
has seven possible parameters:

* **type** - The parameter type. Must be one of: integer, list, boolean, script, string, text, result, object
* **name** - The name of the parameter. This **must** match the name of the parameter on the step function
* **required** - Boolean flag indicating whether this parameter is required to have a value
* **defaultValue** - An optional default value that will be used if a value is not provided
* **language** - An optional script language this parameter expects the value to conform. This should only be present if the **type** is set to *script*
* **className** - If the **type** is **object**, then this should represent the fully qualified class name that is expected
* **parameterType** - An optional attribute that represents the fully qualified class name (or primitive name) of the step function parameter. This may be set regardless of type

##### Branch Steps
Branch steps differ from all other steps in that additional **params** of type **result** may be added which are used to
determine the next step which will be executed. The name of these parameters must match the possible primary return type
value. Only branch steps should have result types and must contain at least one. There is no limit to the number of result
parameters that may be added.

## Pipeline Base
A pipeline has a base structure that is used to execute steps:

```JSON
{
    "id": "b6eed286-9f7e-4ca2-a448-a5ccfbf34e6b",
    "name": "My Pipeline",
    "steps": []
}
```

* **id** - A unique GUID that represents this pipeline
* **name** - A name that may be used in logging during the execution of the pipeline
* **steps** - The list of *pipeline* steps that will be executed

Optionally, a system, may add additional information such as layout (for the steps) and metadata management 
(create, modified date/user).

## Pipeline Steps
A pipeline step begins with a step template, but makes several crucial changes. Below is an example pipeline step.

```JSON
        {
            "id": "LOADFROMPATH",
            "stepId": "87db259d-606e-46eb-b723-82923349640f",
            "nextStepId": "WRITE",
            "displayName": "Load DataFrame from HDFS path",
            "description": "This step will read a dataFrame from the given HDFS path",
            "type": "Pipeline",
            "category": "InputOutput",
            "params": [
                {
                    "type": "text",
                    "name": "path",
                    "required": false,
                    "value": "/tmp/input_file.csv"
                },
                {
                    "type": "object",
                    "name": "options",
                    "required": false,
                    "className": "com.acxiom.pipeline.steps.DataFrameReaderOptions"
                }
            ],
            "engineMeta": {
                "spark": "HDFSSteps.readFromPath",
                "pkg": "com.acxiom.pipeline.steps"
            },
            "tags": [
              "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
              "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
            ]
        }
``` 

### Attribute changes
#### id and stepId 
The id of the step template will be renamed to **stepId** and the **id** will be a string that is unique within the 
pipeline. This id is used for execution order and mapping.

#### nextStepId
The pipeline step will include a new attribute named **nextStepId** which indicates the **id** of the next pipeline step
that should be executed. Branch steps will not have this attribute since the next step id is determined by the **params**
array **result** type parameters.

#### params
At a minimum, the parameter from the step template should be replicated here. Required parameters will have to additionally
set the **value** attribute unless the **defaultValue** attribute is provided. Additional attributes will be required 
based on the selected **type**.

##### Primitive Types
Several primitive types are supported and will be passed in to the step function.

* integer
* boolean
* string

##### text
The text type should be used when the **value** is a string and may use special mapping characters. This may also be used 
when the value is a string.

* **Step Response** - The value will begin with the *@* symbol.
* **Secondary Step Response** - The value will begin with the *#* symbol. This type will most often contain a *.* to drill into the response by name.
* **Global** - The value will begin with a *!* symbol.
* **Pipeline Parameter** - The value will begin with a *$* symbol.
* **Pipeline** - The value will begin with a *&* symbol.

##### object
Objects will be expanded in the case of embedded mapped values. The final value will still be a Map unless the **className**
attribute has been provided. When the **className** has been provided, then the map will be replaced with a populated
object which should match the step function parameter type.

##### list
The **value** may be a list which is represented as a JSON array. The list may contain primitive values, *object* or
mapped values. Mapped values will be expanded just like a **text** type.

Objects will be handled like a parameter of type object.

##### script
The script type should be used when the **value** or **defaultValue** is expected to be executed by the step. Scala, SQL 
and Javascript steps are provided by the **metalus-common** project. The value would be a string which includes the 
proper code. The **language** attribute should be provided with a proper language (javascript, scala, sql or json, etc...).

##### result
The result type should only be used with a branch step. The name of the parameter should match a possible return value
of the step function. The **value** of the parameter must either be an empty string or a valid **id** for a pipeline step.


## Pipeline Final
Below is an example of how a basic two step pipeline may look once complete.

```json
{
    "id": "",
    "name": "",
    "category": "pipeline",
    "steps": [
        {
            "id": "LOADFROMPATH",
            "displayName": "Load DataFrame from HDFS path",
            "description": "This step will read a dataFrame from the given HDFS path",
            "type": "Pipeline",
            "category": "InputOutput",
            "nextStepId": "WRITE",
            "params": [
                {
                    "type": "text",
                    "name": "path",
                    "required": false,
                    "value": "/tmp/input_file.csv"
                },
                {
                    "type": "object",
                    "name": "options",
                    "required": false,
                    "className": "com.acxiom.pipeline.steps.DataFrameReaderOptions"
                }
            ],
            "engineMeta": {
                "spark": "HDFSSteps.readFromPath",
                "pkg": "com.acxiom.pipeline.steps"
            },
            "tags": [
              "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
              "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
            ],
            "stepId": "87db259d-606e-46eb-b723-82923349640f"
        },
        {
            "id": "WRITE",
            "displayName": "Write DataFrame to table using JDBCOptions",
            "description": "This step will write a DataFrame as a table using JDBCOptions",
            "type": "Pipeline",
            "category": "InputOutput",
            "params": [
                {
                    "type": "text",
                    "name": "dataFrame",
                    "required": false,
                    "value": "@LOADFROMPATH"
                },
                {
                    "type": "text",
                    "name": "jdbcOptions",
                    "required": false
                },
                {
                    "type": "text",
                    "name": "saveMode",
                    "required": false
                }
            ],
            "engineMeta": {
                "spark": "JDBCSteps.writeWithJDBCOptions",
                "pkg": "com.acxiom.pipeline.steps"
            },
            "stepId": "c9fddf52-34b1-4216-a049-10c33ccd24ab"
        }
    ]
}
```
