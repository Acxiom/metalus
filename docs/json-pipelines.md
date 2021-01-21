[Documentation Home](readme.md)

# JSON Pipelines
A critical part of any application is the [pipeline(s)](pipelines.md) that gets executed. Building pipelines in JSON 
provides developers a way to construct Spark applications using existing [step libraries](step-libraries.md) without
the need to write and deploy code. Once a pipeline is designed it may be delivered several ways:

* As part of an application JSON
* An API (a custom [PipelineManager](pipeline-manager.md) or DriverSetup would be required)
* Embedded in a jar
* A disk (local/hdfs/s3, etc...) location (a custom [PipelineManager](pipeline-manager.md) or DriverSetup would be required)

The **Metalus** library will then convert the JSON into a scala object to be executed.

There are three components needed to create a pipeline:

* [Step Template](step-templates.md) - This is the JSON that describes the step. Pipeline Steps start with this template and modify to satisfy pipeline requirements.
* Pipeline Step - The step template is copied and modified with additional attributes and mappings before being added to the pipeline JSON.
* Pipeline - Contains information about the pipeline including which steps to execute at runtime.

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

## [Pipeline Steps](pipeline-steps.md)
A pipeline step begins with a step template, but makes several crucial attribute changes. More information can be found 
[here](pipeline-steps.md).

## Pipeline Final
Below is an example of how a basic two step pipeline may look once complete:

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
