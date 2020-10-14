[Documentation Home](readme.md)

# Pipeline Steps
A pipeline step begins with a step template, but makes several crucial changes. Below is a JSON example pipeline step:

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

## Attribute changes
### id and stepId 
The id of the step template will be renamed to **stepId** and the **id** will be a string that is unique within the 
pipeline. This id is used for execution order and mapping.

### nextStepId
The pipeline step will include a new attribute named **nextStepId** which indicates the **id** of the next pipeline step
that should be executed. Branch steps will not have this attribute since the next step id is determined by the **params**
array **result** type parameters.

### params
At a minimum, the parameter from the step template should be replicated here. Required parameters will have to additionally
set the **value** attribute unless the **defaultValue** attribute is provided. Additional attributes will be required 
based on the selected **type**.

#### Primitive Types
Several primitive types are supported and will be passed in to the step function.

* integer
* boolean
* string

#### text
The text type should be used when the **value** is a string and may use special mapping characters. This may also be used 
when the value is a string.

* **Step Response** - The value will begin with the *@* symbol.
* **Secondary Step Response** - The value will begin with the *#* symbol. This type will most often contain a *.* to drill into the response by name.
* **Global** - The value will begin with a *!* symbol.
* **Pipeline Parameter** - The value will begin with a *$* symbol.
* **Pipeline** - The value will begin with a *&* symbol.

#### object
Objects will be expanded in the case of embedded mapped values. The final value will still be a Map unless the **className**
attribute has been provided. When the **className** has been provided, then the map will be replaced with a populated
object which should match the step function parameter type.
#### list
The **value** is a list that may be represented as an array when using JSON. The list may contain primitive values, 
*object* or mapped values. Mapped values will be expanded just like a **text** type.

Objects will be handled like a parameter of type object.
#### script
The script type should be used when the **value** or **defaultValue** is expected to be executed by the step. Scala, SQL 
and Javascript steps are provided by the **metalus-common** project. The value would be a string which includes the 
proper code. The **language** attribute should be provided with a proper language (javascript, scala, sql or json, etc...).
#### scalascript
The scalascript type allows dynamic mappings to be written using Scala. More information can be found [here](step-templates.md#scala-script-parameters).
#### result
The result type should only be used with a branch step. The name of the parameter should match a possible return value
of the step function. The **value** of the parameter must either be an empty string or a valid **id** for a pipeline step.
