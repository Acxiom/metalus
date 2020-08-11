[Documentation Home](readme.md)

# Step Annotations
Pipeline designers that are building JSON representations need step templates as a starting point for each pipeline step
to be used. Once these step templates are created, they need to be stored so that pipeline designers have access. As an
alternative to creating and storing step templates, several annotations have been created to allow step developers the 
ability to add all of the necessary data in code so that pipeline designers can generate the step templates using the 
[metadata extractor utility](metadata-extractor.md).

## Step Object
This annotation must be used on any object that contains a step function. Objects without this annotation will be skipped.

```scala
@StepObject
```

## Step Function
The step function annotation is used to mark any function in an object that may be used in a pipeline. Several attributes
are required:

* **id** - A unique GUID which can be used to identify this step function
* **displayName** - A short name that may be presented to a user in a UI
* **description** - A longer explanation of what this step does
* **type** - The type of step. It can be one of the following values: Pipeline, pipeline, branch, fork, join or step-group
* **category** - Represents a group which this step should belong. This is useful for UIs and queries to provide a common grouping of steps

```scala
@StepFunction("a7e17c9d-6956-4be0-a602-5b5db4d1c08b",
"Scala script Step",
"Executes a script and returns the result",
"Pipeline",
"Scripting")
```

## Step Parameter
The step parameter annotation is an optional way to override metadata that will be derived by default. Several attributes
are required:

* **typeOverride** - The parameter type
* **required** - The required status
* **defaultValue** - Provide a default value for this parameter
* **language** - An optional language used for *script* types
* **className** - The fully qualified class name to use when the type is object
* **parameterType** - The fully qualified class name of the parameter. Option parameters should use the internal type, not Option
* **description** - A description of the parameter.

The **None** option may be used in place of having to provide actual values.

```scala
@StepParameter(Some("script"), Some(true), None, Some("scala"), None, None) script: String
```

## Step Parameters
The step parameters annotation can be used to simplify annotating the parameters in a single spot. The annotation takes 
a single map which uses the parameter name as the key and a _StepParameter_ as the value. Below is an example of an
annotated step function.

```scala
@StepFunction("15889487-fd1c-4c44-b8eb-973c12f91fae",
    "Creates an HttpRestClient",
    "This step will build an HttpRestClient using a host url and optional authorization object",
    "Pipeline",
    "API")
  @StepParameters(Map(
    "hostUrl" -> StepParameter(None, Some(true), None, None, None, None, Some("The URL to connect including port")),
    "authorization" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional authorization class to use when making connections")),
    "allowSelfSignedCertificates" ->
      StepParameter(None, Some(false), None, None, None, None, Some("Flag to allow using self signed certificates for http calls"))))
  def createHttpRestClient(hostUrl: String,
                           authorization: Option[Authorization] = None,
                           allowSelfSignedCertificates: Boolean = false): HttpRestClient = {
    new HttpRestClient(hostUrl, authorization, allowSelfSignedCertificates)
  }
```
## Step Results
The step results annotation is an optional way to specify the return types when a *PipelineStepResponse* is used by the
step function. There are two attributes required:

* **primaryType** - This should be the fully qualified class name of the primary response type. Option parameters should use the internal type, not Option
* **secondaryTypes** - This is an optional map that contains the name of each parameter and the expected type

```scala
@StepResults(primaryType = "com.acxiom.pipeline.PipelineStepResponse", secondaryTypes = None)
```

```scala
@StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = Map("schema" -> "com.acxiom.pipeline.steps.Schema"))
```

## Branch Results
The branch results annotation is an optional way to list the expected response values that the step function may return.
There is only one required attribute:

* **names** - A list of names that can be expected from the step function

```scala
@BranchResults(List("true", "false", "something-else"))
```

## Example

**Basic requirements**:
```scala
@StepObject
object HDFSSteps {
  @StepFunction("87db259d-606e-46eb-b723-82923349640f",
    "Load DataFrame from HDFS path",
    "This step will read a dataFrame from the given HDFS path",
    "Pipeline",
    "InputOutput")
  def readFromPath(path: String,
                   options: DataFrameReaderOptions = DataFrameReaderOptions(),
                   pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(options, pipelineContext).load(path)
  }
}
```

**Step Parameter**:
```scala
@StepObject
object ScalaSteps {
  private val logger = Logger.getLogger(getClass)
  @StepFunction("a7e17c9d-6956-4be0-a602-5b5db4d1c08b",
    "Scala script Step",
    "Executes a script and returns the result",
    "Pipeline",
    "Scripting")
  def processScript(@StepParameter(Some("script"), Some(true), None, Some("scala"), None, None) script: String,
                    pipelineContext: PipelineContext): PipelineStepResponse = {
    val engine = new ScalaScriptEngine
    val result = engine.executeScript(script, pipelineContext)
    handleResult(result)
  }
}
```

**Branch Results**:
```scala
@StepObject
object BranchSteps {
  @StepFunction("f32647a5-b4b1-4eb8-92b1-51a23c828365",
    "Decide something",
    "Handles returning a decision",
    "branch",
    "Decision")
  @BranchResults(List("true", "false"))
  def branchFunction(test: Boolean): String = {
    if (test) {
      "true"
    } else {
      "false"
    } 
  }
}
```

**Step Results**:
```scala
@StepObject
object ResultSteps {
  @StepFunction("315972d9-316f-46d0-8111-277b7837ea2f",
    "Return something",
    "Handles returning stuff",
    "Pipeline",
    "Test")
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = Map("schema" -> "com.acxiom.pipeline.steps.Schema"))
  def someFunction(path: String, pipelineContext: PipelineContext): PipelineStepResponse = {
    val df = DataFrameSteps.getDataFrameReader(DataFrameReaderOptions(), pipelineContext).load(path)
    PipelineStepresponse(Some(df), Some(Map("schema" -> Schema.fromStructType(df.schema))))
  }
}
```
