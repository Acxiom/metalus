[Home](../readme.md)

# Metalus Core
The core library provides the foundational components required to build application workflows using JSON. Each application
is made up of a root pipeline, contexts, mapped parameters, pipeline templates, an event listener and a parameter mapper.

## Terminology:
* An _**Application**_ defines the setup required to execute a workflow. An _Application_ may be reusable, or custom 
to a single execution.
* A _**Pipeline**_ is the main definition of a workflow. Designed to be reusable, pipelines rely heavily on runtime mappings.
* A _**Step**_ is a component of a _Pipeline_ and represents a single unit of work. _Steps_ take two forms, a template that
represents the parameters which must be filled in when building a _Pipeline_, and a _FlowStep_ which is part of the _Pipeline_
and contains either static values or _mapped values_ to be resolved at runtime.
* _**Contexts**_ are used to inject new functionality within an _Application_ without having to force all code to be tied to
that technology. An example is the _SparkSessionContext_. This context should only be configured if the application is
being executed within a Spark server.
* _**Mapped Parameters**_ are values that can be referenced by the _FlowSteps_ at runtime. These parameters can be defined
in the _Application_ JSON sections _globals_ or _pipelineParameters_. These will be referenced differently when they
are _mapped_ in a _FlowStep_ parameter. See the [Parameter Mapping](docs/flow-step-parameter-mapping.md) section for
more information. In addition to definitions within the JSON, parameters may be specified on the command line. These
parameters must start with _--_. The name will be used as the key and the value will stored within the _globals_ on the
_PipelineContext_ and made available to all executed _Pipelines_.
* The _**PipelineListener**_ is used to generate events providing some insight into the workflow at runtime. A default
implementation will log these events or a custom implementation may be defined in the _pipelineListener_ section of the JSON.
* The _**PipelineStepMapper**_ provides a pluggable mechanism for handling the _FlowStep_ parameter mappings. This allows
_Pipelines_ to and steps to be created once and then reused by simply providing different _Mapped Parameters_ at runtime.
* _**Connectors**_ are another mechanism provided to make _Pipelines_ and _FlowSteps_ reusable by allowing the connection
information and behaviors to be defined at runtime. There are three types of Connectors: _FILE_, _DATA_ and _STREAM_.
* **_Execution Engines_** are tags that can be used by the workflow process to determine which steps to execute ate runtime as
well as tag which execution environments the flow is being processed.
* _**Data References**_ are a mechanism for working with data across execution engines. [Data References](docs/data-references.md) allow data that exists
in different environments (Spark, JDBC, InMemory) to be joined and then determine which engine to run the data upon
execution of the query.
* _**Session Management**_ allows applications to restart and recover processes without having to restart from the beginning.
The application pipeline and child pipelines should define restart steps to make this process easier. 

## Application Sections:
### Root Pipeline
The _pipelineId_ attribute defines the main flow to be executed. This pipeline may exist on the classpath or be defined in the
_pipelineTemplates_ section of the application JSON definition.

### Pipeline Templates
The _pipelineTemplates_ attribute is an array that defines _Pipelines_ that will be referenced at runtime. _Pipelines_ defined here will
override any _Pipeline_ that is found on the classpath with the same id.

### Step Packages
The _stepPackages_ attribute is an array which specifies where the flow engine should look to locate step objects. These
are the default locations and can be overridden by the _FlowStep_. If this attribute is provided, it will override the
defaults. The default values if not specified are:
* com.acxiom.metalus
* com.acxiom.metalus.steps
* com.acxiom.metalus.pipeline.steps

If additional libraries are used such as the Spark library, an additional package would be needed like: _com.acxiom.metalus.spark.steps_

### Globals
The _globals_ section provides a place to define objects, connectors and parameters that will be used by the executing
_Pipelines_ at runtime. These objects may also contain mappings from command line parameters to allow better reusability
of applications.

### Pipeline Listener
The _pipelineListener_ section provides a mechanism for injecting a custom listener. Classes must extend the _PipelineListener_
interface. The class that is specified here will be the only listener allowed. _CombinedPipelineListener_ is provided as
a way to have multiple listeners specified. An example would be to use the _DefaultPipelineListener_ for logging and
a custom listener that implements the _EventBasedPipelineListener_ to have events sent to an event queue.

### Pipeline Step Mapper
The _stepMapper_ section is used to define the [Pipeline Step Mapper](docs/flow-step-parameter-mapping.md) that will be used for runtime parameter mapping. It 
is recommended that the default mapper be used or the new expression mapper. There are also extension points that can be
overridden by extending the default implementation.

### Pipeline Parameters
The _pipelineParameters_ section is used to define runtime parameters that should only be applied to a specific _Pipeline_
and/or _FlowStep_ within a _Pipeline_. Unlike _globals_, the same parameter name may be used as long as the pipeline id is
different.

### Required Parameters
The _requiredParameters_ section is used to define which parameters are required to be provided by the command line. Users
should specify the names here if they are expecting a parameter on the command line that is being used in a mapping or as
part of global object creation during startup.

### Pipeline Manager
The _pipelineManager_ section allows overriding the _PipelineManager_ implementation. The default implementation is the
_CachedPipelineManager_ which is preloaded with all _Pipelines_ defined in the [Pipeline Templates Section](#pipeline-templates).
It is recommended to use this class as it will use the defined _Pipelines_ first and then look on teh classpath
for the _Pipeline_.

### Contexts
The _contexts_ section is where different _Context_ implementations may be defined for the _ContextManager_. The most
common examples are the _Json4sContext_(default) which manages JSON serialization and the _SessionContext_ which is used
to manage workflow state. The Spark project provides a _SparkSessionContext_ to enable working with the SparkSession. 
