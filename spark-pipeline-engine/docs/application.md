# Applications
The *Application* concept allows for the creation of an application by providing a JSON configuration document. Options
are provided that allow customization of the *PipelineContext*, *SparkConf*, global variables and pipelines.

## Running
There are two ways to run an application. Depending on where the configuration file is located dictates which command
line parameters need to be provided. There are two sets of parameters that are used depending on where the file is located:

* **applicationJson** - This parameter is useful when passing the contents of the configuration as a string. This is not 
a recommended option since large configurations could have issues.
* **applicationConfigPath** - This is the path to the configuration file on the file system. 
* **applicationConfigurationLoader** - This is used to specify a different file manager. Default is local.

### Local Disk
There are two ways to pull the configuration from the local disk, the first is using the *applicationJson* property. The 
second method involves populating the *applicationConfigPath* property and either not providing a value for the
*applicationConfigurationLoader* property or using the value *com.acxiom.pipeline.utils.LocalFileManager*.

**Example:**
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars <jar_path>/spark-pipeline-engine_2.11-<VERSION>.jar,<jar_path>/streaming-pipeline-drivers_2.11-<VERSION>.jar \
<jar_path>/<uber-jar>.jar \
--driverSetupClass com.acxiom.pipeline.applications.ApplicationDriverSetup \
--applicationConfigPath <location of iconfiguration json> \
--logLevel DEBUG
```

### HDFS
Should the configuration be stored on an HDFS file system, then the path should be provided using the *applicationConfigPath* 
property and pass the value *com.acxiom.pipeline.utils.HDFSFileManager* in the *applicationConfigurationLoader* property.

**Example:**
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master yarn \
--deploy-mode cluster \
--jars <jar_path>/spark-pipeline-engine_2.11-<VERSION>.jar,<jar_path>/streaming-pipeline-drivers_2.11-<VERSION>.jar \
<jar_path>/<uber-jar>.jar \
--driverSetupClass com.acxiom.pipeline.applications.ApplicationDriverSetup \
--applicationConfigPath <location of configuration json> \
--applicationConfigurationLoader com.acxiom.pipeline.utils.HDFSFileManager \
--logLevel DEBUG
```

## sparkConf
The *sparkConf* element provides the ability to control which classes are passed to the KryoSerializer and any 
additional settings that should be added to the configuration.

### kryoClasses
The *kryoClasses* array will be used to create an array of classes that are passed to the *SparkConf* function
*registerKryoClasses*.

### setOptions
The *setOptions* array will be used to make calls to the *SparkConf* *set* function passing in the *name* and *value* 
for each element in the array.

```json
{
  "sparkConf": {
    "kyroClasses": [
      "org.apache.hadoop.io.LongWritable",
      "org.apache.http.client.entity.UrlEncodedFormEntity"
    ],
    "setOptions": [
      {
        "name": "spark.hadoop.io.compression.codecs",
        "value": "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.GzipCodec"
      }
    ]
  }
}
```

## requiredParameters
A list of parameter names to validate are present as part of the submit command. Should any required parameter be missing,
then the application will exit. The value is not validated, just the presence of the parameter.

```json
{
  "requiredParameters": [
    "param1",
    "param2"
  ]
}
```

## Global PipelineContext Overrides
The following settings can be configured to use when creating the *PipelineContext* for each defined execution when
the execution does not specify the setting.

### stepPackages
This array is used to set the packages where compiled steps can be found.

### pipelineListener
This object allows specifying a custom *PipelineListener*. The class represented by the fully qualified *className* 
must be available on the class path. The optional *parameters* specified will be passed to the constructor. Currently
only simple types and maps are supported, but more complex data types will be supported in the future.

```json
{
  "pipelineListener": {
    "className": "com.acxiom.pipeline.applications.TestPipelineListener",
    "parameters": {
      "name": "Test Pipeline Listener"
    }
  }
}
```

### securityManager
This object allows specifying a custom *PipelineSecurityManager*. The class represented by the fully qualified *className* 
must be available on the class path. The optional *parameters* specified will be passed to the constructor. Currently
only simple types and maps are supported, but more complex data types will be supported in the future.

```json
{
  "securityManager": {
    "className": "com.acxiom.pipeline.applications.TestPipelineSecurityManager",
    "parameters": {
      "name": "Test Security Manager"
    }
  }
}
```

### stepMapper
This object allows specifying a custom *PipelineStepMapper*. The class represented by the fully qualified *className* 
must be available on the class path. The optional *parameters* specified will be passed to the constructor. Currently
only simple types and maps are supported, but more complex data types will be supported in the future.

```json
{
  "stepMapper": {
    "className": "com.acxiom.pipeline.applications.TestPipelineStepMapper",
    "parameters": {
      "name": "Test Step Mapper"
    }
  }
}
```

### pipelineParameters
This object allows specifying an initial set of *PipelineParameters*.

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

### pipelines
A list of pipelines definitions that can be referenced in any defined execution.

```json
{
  "pipelines": [
    {
      "id": "Pipeline1",
      "name": "Pipeline 1",
      "steps": [
        {
          "id": "Pipeline1Step1",
          "displayName": "Pipeline1Step1",
          "type": "preload",
          "params": [
            {
              "type": "text",
              "name": "value",
              "required": true,
              "value": "!mappedObject"
            }
          ],
          "engineMeta": {
            "spark": "ExecutionSteps.normalFunction"
          }
        }
      ]
    }
  ]
 }
``` 

### globals
The globals object will be merged with the application parameters passed to the 'spark-submit' command. All elements are
added to the *PipelineContext* globals lookup *as-is* unless it is an object **and** contains the *className* property. 
When the *className* element is present, the *Application* loader will take the object specified as the *object* element 
and convert to the case class specified by the *className* element. The newly instantiated object will then be added to
the *PipelineContext* globals lookup using name of the upper level element.

As an example, the *number*, *float* and *string* elements will be added to the  *PipelineContext* globals lookup with
no conversion. However, the *mappedObject* element will have the *object* converted to a *TestGlobalObject* and added
to the lookup with the name *mappedObject*.

```json
{
  "globals": {
    "number": 1,
    "float": 1.5,
    "string": "some string",
    "mappedObject": {
      "className": "com.acxiom.pipeline.applications.TestGlobalObject",
      "object": {
        "name": "Global Mapped Object",
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

## executions
The *executions* array is used to define how the application will execute. Each execution contains several settings:

* **id** - This is used to uniquely identify the execution at runtime.
* **pipelines** - An array containing one or more pipeline definitions that can override globally defined pipelines.
* **pipelineIds** - An array of pipeline ids to execute.
* **parents** - A list of execution *id*s upon which this execution is dependent.

In addition, any of the global *PipelineContext* settings listed above may be defined which will override the global
definitions.

```json
{
  "executions": [
    {
      "id": "0",
      "pipelineIds": ["Pipeline1"],
      "pipelines": [
        {
          "id": "Pipeline1",
          "name": "Pipeline 1",
          "steps": [
            {
              "id": "Pipeline1Step1",
              "displayName": "Pipeline1Step1",
              "type": "preload",
              "params": [
                {
                  "type": "text",
                  "name": "value",
                  "required": true,
                  "value": "!mappedObject"
                }
              ],
              "engineMeta": {
                "spark": "ExecutionSteps.normalFunction"
              }
            }
          ]
        }
      ],
      "securityManager": {
        "className": "com.acxiom.pipeline.applications.TestPipelineSecurityManager",
        "parameters": {
          "name": "Sub Security Manager"
        }
      },
      "globals": {
        "number": 2,
        "float": 3.5,
        "string": "sub string",
        "mappedObject": {
          "className": "com.acxiom.pipeline.applications.TestGlobalObject",
          "object": {
            "name": "Execution Mapped Object",
            "subObjects": [
              {
                "name": "Sub Object 1a"
              },
              {
                "name": "Sub Object 2a"
              },
              {
                "name": "Sub Object 3"
              }
            ]
          }
        }
      }
    },
    {
      "id": "1",
      "pipelineIds": ["Pipeline2"],
      "pipelines": [
        {
          "id": "Pipeline2",
          "name": "Pipeline 2",
          "steps": [
            {
              "id": "Pipeline2Step1",
              "displayName": "Pipeline2Step1",
              "type": "preload",
              "params": [
                {
                  "type": "text",
                  "name": "value",
                  "required": true,
                  "value": "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn"
                }
              ],
              "engineMeta": {
                "spark": "ExecutionSteps.normalFunction"
              }
            }
          ]
        }
      ],
      "parents": [
        "0"
      ],
      "pipelineListener": {
        "className": "com.acxiom.pipeline.applications.TestPipelineListener",
        "parameters": {
          "name": "Sub Pipeline Listener"
        }
      },
      "stepMapper": {
        "className": "com.acxiom.pipeline.applications.TestPipelineStepMapper",
        "parameters": {
          "name": "Sub Step Mapper"
        }
      },
      "pipelineParameters": {
        "parameters":[
          {
            "pipelineId": "Pipeline2",
            "parameters": {
              "howard": "johnson"
            }
          }
        ]
      }
    }
  ]
}
```


