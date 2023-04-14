[Home](../readme.md)

# Metalus Core
The core library provides the foundational components required to build application workflows using JSON. Each application
is made up of a root pipeline, contexts, mapped parameters, pipeline templates, an event listener and a parameter mapper.

The primary goal of the Metalus flow engine is to provide the ability to build workflows that are reusable rather than
specific to a single task. One such task is to copy a file. These steps in this task would be:
* Verify source file exists
* Copy to destination
* Verify copy was successful

Additionally, if the copy fails, we want to:
* Delete partial copy
* Retry

All of these steps are very common and are perfect for reusable functionality. Metalus [pipelines](#pipeline) are designed
to allow building these reusable workflows by combining existing steps (and other pipelines).

The next issue is that when copying a file the file system forces the use of different commands. HDFS, S3, GCS, SFTP and
the local file system all uses different commands. Metalus offers a [connectors](#connectors) API to abstract the 
file system until runtime allowing the same pipeline to be used regardless of file system. This API also allows copying
from one file system to another without having to write a new pipeline.

As with steps and pipelines, [applications](#application) also may be made reusable by mapping in command line parameters
at runtime as a way to construct a work flow once and use it many times.

## Concepts
* [Application](#application)
* [Pipeline](#pipeline)
* [Step](#step)
  * [Step Template](#step-template)
  * [Flow Step](#flowstep)
* [Mapped Parameters](#mapped-parameters)
* [Execution Engines](#execution-engines)
* [Connectors](#connectors)
* [Data References](#data-references)
* [Contexts](#contexts)
* [Session Management](#session-management)
* [Pipeline State Keys](#pipeline-state-keys)
* [Credentials](#credentials)
### Application
An [Application](docs/application.md) defines the setup required to execute a workflow. The _Application_ definition is
the highest level workflow component. There are sections that set up the workflow, but there is only a single [pipeline](#pipeline)
for the main flow. This _pipeline_ may be a self-contained flow or it may embed other pipelines.
### Pipeline
A [pipeline](docs/pipelines.md) is the mechanism used to process work. Pipelines can also embed other pipelines to allow
reuse where it makes sense.
### Step
A [step](docs/steps.md) defines a unit of work within a [pipeline](#pipeline). The term step is used to describe two separate
concepts:
#### Step Template
A step template represents the metadata about a step. This metadata is useful for building [FlowSteps](#flowstep) within
a [pipeline](#pipeline).
#### FlowStep
A flow step is any step that has been added to a [pipeline](#pipeline). The metadata is very similar, but additional
attributes are added to the step and the parameters.
### Mapped parameters
Mapped Parameters are values that can be referenced by the [FlowSteps](#flowstep) at runtime. These parameters can be defined
in the [Application](#application) JSON sections [globals](docs/application.md#globals) or [pipelineParameters](docs/application.md#pipeline-parameters).
These will be referenced differently when they are _mapped_ in a [FlowStep](#flowstep) parameter. See the
[Parameter Mapping](docs/flow-step-parameter-mapping.md) section for more information. In addition to definitions within
the JSON, parameters may be specified on the command line. These parameters must start with _--_. The name will be used
as the key and the value will be stored within the [globals](docs/application.md#globals) on the _PipelineContext_ and made
available to all executed [Pipelines](#pipeline).
### Execution Engines
Execution Engines are tags that can be used by the workflow process to determine which steps to execute ate runtime as
well as tag which execution environments the flow is being processed.
### Connectors
[Connectors](docs/connectors.md) are another mechanism provided to make [Pipelines](#pipeline) and [FlowSteps](#flowstep) reusable by allowing
the connection information and behaviors to be defined at runtime. There are three types of Connectors:
_FILE_, _DATA_ and _STREAM_.
### Data References
Data References are a mechanism for working with data across [execution engines](#execution-engines). [Data References](docs/data-references.md)
allow data that exists in different environments (Spark, JDBC, InMemory) to be joined and then determine which engine to
run the data upon execution of the query. The concept is similar to Spark DataFrames but without the requirement to run
within a Spark environment.
### Contexts
In an effort to reduce coupling to specific technologies, _Contexts_ are used to provide a simple interface for introducing
functional components into the system at runtime. Examples include JSON4S, Spark and Session management.
### Session Management
[Session Management](docs/session-management.md) allows applications to restart and recover processes without having to
start from the beginning. The application pipeline and child pipelines should define restart steps to make this process
easier.
### Pipeline State Keys
A [universal key](docs/pipelines.md#pipeline-state-key) allows every pipeline, step, step response, pipeline parameter and audit can be uniquely be identified.
These keys are specific enough that a specific instance of a step running in a fork may be identified. This universal key
facilitates accurately recovering failed processes, restarting steps, mapping flowstep parameter values and providing
detailed audits.
### Credentials
The [Credential Provider](docs/credential-provider.md) API is provided to allow managing secure credentials at runtime
without the need to store unsecured authentication information within JSON files. The AWS and GCP projects provide
secrets manager providers for the respective cloud.
