[Home](../readme.md)

# Applications
An _**Application**_ defines the setup required to execute a workflow. An _Application_ may be
reusable or custom to a single execution.

* [Application Sections](#application-sections-)
* [Running](#running)

## Application Sections:
### Root Pipeline
The _pipelineId_ attribute defines the main flow to be executed. This [pipeline](docs/pipeline.md) may exist on the classpath or be defined in the
_pipelineTemplates_ section of the application JSON definition.

### Pipeline Templates
The _pipelineTemplates_ attribute is an array that defines _Pipelines_ that will be referenced at runtime. _Pipelines_ defined here will
override any _Pipeline_ that is found on the classpath with the same id. Below is an example:

```json
{
  "pipelineTemplates": [
    {
      "id": "Pipeline1",
      "name": "Pipeline 1",
      "steps": [
        {
          "id": "Pipeline1Step1",
          "displayName": "Pipeline1Step1",
          "type": "Pipeline",
          "params": [
            {
              "type": "text",
              "name": "value",
              "required": true,
              "value": "!mappedObject"
            }
          ],
          "engineMeta": {
            "command": "ExecutionSteps.normalFunction"
          }
        }
      ]
    }
  ]
 }
```

### Step Packages
The _stepPackages_ attribute is an array which specifies where the flow engine should look to locate step objects. These
are the default locations and can be overridden by the _FlowStep_. If this attribute is provided, it will override the
defaults. The default values if not specified are:
* com.acxiom.metalus
* com.acxiom.metalus.steps
* com.acxiom.metalus.pipeline.steps

If additional libraries are used such as the Spark library, an additional package would be needed like:

```json
{
  "stepPackages": [
    "com.acxiom.metalus.spark.steps",
    "com.acxiom.metalus",
    "com.acxiom.metalus.steps",
    "com.acxiom.metalus.pipeline.steps"
  ]
}
```

Note that the spark package was added before the defaults to ensure that it is considered first.

### Globals
The _globals_ section provides a place to define objects, connectors and parameters that will be used by the executing
_Pipelines_ at runtime. These objects may also contain mappings from command line parameters to allow better reusability
of applications. Below is an example of the _globals_ section:

```json
{
  "globals": {
    "number": 1,
    "float": 1.5,
    "string": "some string",
    "mappedObject": {
      "className": "com.acxiom.pipeline.applications.TestGlobalObject",
      "mapEmbeddedVariables": true,
      "object": {
        "name": "Global Mapped Object",
        "mappedParam": "!commandLineValue",
        "subObjects": [
          {
            "name": "Sub Object 1"
          },
          {
            "name": "Sub Object 2"
          }
        ]
      }
    }
  }
}
```
#### Object Mapping
Note that the _mappedObject_ will be converted to an instance of _TestGlobalObject_. It will be available in the _globals_
under the key _mappedObject_. The attribute _className_ is used to specify the class that represents the object
(not an interface or trait) and the object provides the class parameters. A third parameter named _mapEmbeddedVariables_
is used to specify whether command line parameters should be mapped into the JSON prior to materializing as an object.

### Pipeline Listener
The _pipelineListener_ section provides a mechanism for injecting a custom listener. Classes must extend the _PipelineListener_
interface. The class that is specified here will be the only listener allowed. _CombinedPipelineListener_ is provided as
a way to have multiple listeners specified. An example would be to use the _DefaultPipelineListener_ for logging and
a custom listener that implements the _EventBasedPipelineListener_ to have events sent to an event queue. Below
is an example of defining the default pipeline listener:

```json
{
  "pipelineListener": {
    "className": "com.acxiom.metalus.DefaultPipelineListener",
    "parameters": {
      "name": "Default Pipeline Listener"
    }
  }
}
```

### Pipeline Step Mapper
The _stepMapper_ section is used to define the [Pipeline Step Mapper](docs/flow-step-parameter-mapping.md) that will be used for runtime parameter mapping. It
is recommended that the default mapper be used or the new expression mapper. There are also extension points that can be
overridden by extending the default implementation. Below is an example of how to define a custom mapper:

```json
{
  "stepMapper": {
    "className": "com.acxiom.metalus.MyPipelineStepMapper",
    "parameters": {
      "name": "My Step Mapper"
    }
  }
}
```

### Pipeline Parameters
The _pipelineParameters_ section is used to define runtime parameters that should only be applied to a specific _Pipeline_
and/or _FlowStep_ within a _Pipeline_. Unlike _globals_, the same parameter name may be used as long as the pipeline id is
different. Below is an example definition for a parameter named _fred_ to be used by the pipeline _Pipeline1_:

```json
{
  "pipelineParameters": {
    "parameters":[
      {
        "pipelineId": "Pipeline1",
        "parameters": {
          "fred": "johnson"
        }
      }
    ]
  }
}
```

### Required Parameters
The _requiredParameters_ section is used to define which parameters are required to be provided by the command line. Users
should specify the names here if they are expecting a parameter on the command line that is being used in a mapping or as
part of global object creation during startup. Below is an example:

```json
{
  "requiredParameters": [
    "param1",
    "param2"
  ]
}
```

### Pipeline Manager
The _pipelineManager_ section allows overriding the _PipelineManager_ implementation. The default implementation is the
_CachedPipelineManager_ which is preloaded with all _Pipelines_ defined in the [Pipeline Templates Section](#pipeline-templates).
It is recommended to use this class as it will use the defined _Pipelines_ first and then look on teh classpath
for the _Pipeline_.

### Contexts
The _contexts_ section is where different _Context_ implementations may be defined for the _ContextManager_. The most
common examples are the _Json4sContext_(default) which manages JSON serialization and the _SessionContext_ which is used
to manage workflow state. The Spark project provides a _SparkSessionContext_ to enable working with the SparkSession.

## Running
There are two ways to run an application. Depending on where the configuration file is located dictates which command
line parameters need to be provided. There are several parameters that are used depending on where the file is located:

* **applicationId** - This will attempt to load the application JSON from the classpath.
* **applicationJson** - This parameter is useful when passing the contents of the configuration as a string. This is not 
a recommended option since large configurations could have issues.
* **applicationConfigPath** - This is the path to the configuration file on the file system. 
* **applicationConfigurationLoader** - This is used to specify a different file manager. Default is local.
* **authorization.class** - This is used to specify an authorization implementation when the *applicationConfigPath* is
a URL. Any parameters required to instantiate the *Authorization* instance need to be prefixed with *authorization.* so
they are identified and passed in. To use basic authorization, consider this class signature: 

```scala
class BasicAuthorization(username: String, password: String) extends Authorization
```

would need these command line parameters:

```
--application.class com.acxiom.pipeline.api.BasicAuthorization --authorization.username myuser --authorization.password mypasswd
```

### Local Disk
There are two ways to pull the configuration from the local disk, the first is using the *applicationJson* property. The 
second method involves populating the *applicationConfigPath* property and either not providing a value for the
*applicationConfigurationLoader* property or using the value *com.acxiom.metalus.fs.LocalFileManager*.

**Example:**

```bash
scala -classpath <jar_path>/metalus-core_2.13-<VERSION>.jar \
com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--applicationConfigPath <location of iconfiguration json> \
--logLevel DEBUG
```
It is recommended to download an [SLF4J](https://www.slf4j.org/manual.html#swapping) jar to activate logging.
