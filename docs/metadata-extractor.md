[Documentation Home](readme.md)

# Metadata Extractor
The MetadataExtractor is a generic tool which will scan the provided jar files and extract specific metadata. The 
StepMetadataExtractor and PipelineMetadataExtractor will be executed by default and additional extractors can be 
executed as long as the classes are part of the provided jars and implement the **Extractor** trait. The default extractors
can be disabled by setting the flags *excludePipelines* and *excludeSteps* to true.

## Built-in Extractors

### Step Metadata Extractor
This extractor will scan jar files that contain steps and produce a JSON representation. The tool takes a list of packages
and identifies objects with the annotation *StepObject*. Next the tool will look for any function annotated with the
*StepFunction* annotation. Once a function has been identified, the parameters will be inspected to produce the **params**
array. One additional annotation named *StepParameter* may be used to provide overrides:

* **type** - This allows setting the type to **script** or **object** when the parser may miss it because the parameter is an option or string
* **required** - Override the required parsing
* **defaultValue** - Set a default value
* **language** - This is only useful if the type is overridden to be **script**
* **className** - Only set this if the case class type is wrapped in an Option. Do not override for any other types such as List.

The output will contain the *pkgs* array with all of the packages that contained steps, the*steps* array that contains 
the metadata generated for each step, and the *pkgObjs* array that contains schemas for all of the case classes used as 
parameters in each step.

**Note:** When using annotations, all parameters must be supplied. Named parameters will not work.

### Pipeline Metadata Extractor
This extractor will scan jar files looking for JSON files stored under the *metadata/pipelines* path. Each pipeline will
be loaded and reconciled to a list. This list will be written to the *pipelines.json* file or posted to the */api/v1/pipelines*
API end point.

## Creating a Custom Extractor
Custom extractors may be created and used with or in place of the existing extractors. Three things are required in order
to use a custom extractor:

* **Custom Extractor class** - A class that extends the _com.acxiom.metalus.Extractor_ trait.
* **Custom Extractor jar** - A jar file containing the custom extractor class. This jar should be copied to the 
_metalus-utils/libraries_ directory.
* **Command line parameter** - The _--extractors_ command line switch.

### Custom Extractor Class
A custom extractor must extend the _com.acxiom.metalus.Extractor_ trait. Two functions are required to be overridden:

* **getMetaDataType** - This should return the type of extractor. This will also be used to build out URLs when push to 
an API unless the _writeOutput_ is overridden to handle output.
* **extractMetadata** - Contains the logic to generate the _Metadata_ object used to write the output. A custom _Metadata_
object may be returned of the _JsonMetaData_ may be used. It is expected that the contents of _Metadata.value_ are a 
valid JSON string. 

An example extractor:
```scala
import com.acxiom.metalus.{Extractor, Metadata, Output}
class ExampleMetadataExtractor extends Extractor {

  override def getMetaDataType: String = "examples"

  override def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val exampleMetadata = jarFiles.foldLeft(List[Example]())((examples, file) => {
      val updatedExamples = file.entries().toList
        .filter(f => f.getName.startsWith("metadata/examples") && f.getName.endsWith(".json"))
        .foldLeft(examples)((exampleList, json) => {
          val example = DriverUtils.parseJson(
              Source.fromInputStream(
                file.getInputStream(file.getEntry(json.getName))).mkString, "com.acxiom.example.Example").asInstanceOf[Example]
          if (example.isDefined) {
            exampleList.foldLeft(example.get)((examples, example) => {
              if (examples.exists(p => p.id == example.id)) {
                examples
              } else {
                examples :+ example
              }
            })
            exampleList ::: example.get
          } else {
            exampleList
          }
        })
      examples ::: updatedExamples
    })
    PipelineMetadata(Serialization.write(exampleMetadata), exampleMetadata)
  }

  // Only override to provide custom output handling. This example overrides standard API handling.
  override def writeOutput(metadata: Metadata, output: Output): Unit = {
    if (output.api.isDefined) {
      val http = output.api.get
      val definition = metadata.asInstanceOf[ExampleMetadata]
      definition.examples.foreach(example => {
        if (http.exists(s"/examples/${example.id}")) {
          http.putJsonContent(s"/examples/${example.id}", Serialization.write(example))
        } else {
          http.putJsonContent("/examples", Serialization.write(example))
        }
      })
    } else {
      super.writeOutputFile(metadata, output)
    }
  }
}

case class ExampleMetadata(value: String, examples: List[Example]) extends Metadata
```

## Running
The script parameters are:
* --jar-files - A comma separated list of jar files. This should be the full path.
* --api-url The base URL to use when pushing data to an API. This parameter is optional.
* --api-path The base path to use when pushing data to an API. This parameter is optional and defaults to '/api/v1'.
* --output-path - A path to write the JSON output. This parameter is optional.
* --extractors - An optional comma separated list of extractor class names.

**Authorization**:
When pushing metadata to an API, [authorization](httprestclient.md#authorization) is not used unless the authorization 
parameters are provided.

Installation:
* Download the tar file from the releases page
* Expand the tar file (tar xzf metalus-utils_2.11-spark_2.3...)
* Change to the bin directory (cd metalus-utils/bin)
* Example commands:

Write to a file:
```bash
./metadata-extractor.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp
```

Write to an api:
```bash
./metadata-extractor.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --api-url http://localhost:8000
```

Write to a file with an additional Extractor:
```bash
./metadata-extractor.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --output-path /tmp --extractors com.acxiom.metalus.MyExampleExtractor
```

## Example using common steps

```json
{
  "pkgs": [
    "com.acxiom.pipeline.steps"
  ],
  "steps": [
    {
      "id": "3806f23b-478c-4054-b6c1-37f11db58d38",
      "displayName": "Read a DataFrame from Hive",
      "description": "This step will read a dataFrame in a given format from Hive",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "table",
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
        "spark": "HiveSteps.readDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
      "displayName": "Write DataFrame to Hive",
      "description": "This step will write a dataFrame in a given format to Hive",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "table",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "object",
          "name": "options",
          "required": false,
          "className": "com.acxiom.pipeline.steps.DataFrameWriterOptions",
          "parameterType": "com.acxiom.pipeline.steps.DataFrameWriterOptions"
        }
      ],
      "engineMeta": {
        "spark": "HiveSteps.writeDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
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
    },
    {
      "id": "8daea683-ecde-44ce-988e-41630d251cb8",
      "displayName": "Load DataFrame from HDFS paths",
      "description": "This step will read a dataFrame from the given HDFS paths",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "paths",
          "required": false,
          "parameterType": "scala.List[String]"
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
        "spark": "HDFSSteps.readFromPaths",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "0a296858-e8b7-43dd-9f55-88d00a7cd8fa",
      "displayName": "Write DataFrame to HDFS",
      "description": "This step will write a dataFrame in a given format to HDFS",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
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
          "className": "com.acxiom.pipeline.steps.DataFrameWriterOptions",
          "parameterType": "com.acxiom.pipeline.steps.DataFrameWriterOptions"
        }
      ],
      "engineMeta": {
        "spark": "HDFSSteps.writeToPath",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "e4dad367-a506-5afd-86c0-82c2cf5cd15c",
      "displayName": "Create HDFS FileManager",
      "description": "Simple function to generate the HDFSFileManager for the local HDFS file system",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [],
      "engineMeta": {
        "spark": "HDFSSteps.createFileManager",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Option[com.acxiom.pipeline.fs.HDFSFileManager]"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "a7e17c9d-6956-4be0-a602-5b5db4d1c08b",
      "displayName": "Scala script Step",
      "description": "Executes a script and returns the result",
      "type": "Pipeline",
      "category": "Scripting",
      "params": [
        {
          "type": "script",
          "name": "script",
          "required": false,
          "language": "scala",
          "className": "String"
        }
      ],
      "engineMeta": {
        "spark": "ScalaSteps.processScript",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.PipelineStepResponse"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "8bf8cef6-cf32-4d85-99f4-e4687a142f84",
      "displayName": "Scala script Step with additional object provided",
      "description": "Executes a script with the provided object and returns the result",
      "type": "Pipeline",
      "category": "Scripting",
      "params": [
        {
          "type": "script",
          "name": "script",
          "required": false,
          "language": "scala",
          "className": "String"
        },
        {
          "type": "text",
          "name": "value",
          "required": false,
          "parameterType": "scala.Any"
        },
        {
          "type": "text",
          "name": "type",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "ScalaSteps.processScriptWithValue",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.PipelineStepResponse"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "cdb332e3-9ea4-4c96-8b29-c1d74287656c",
      "displayName": "Load table as DataFrame using JDBCOptions",
      "description": "This step will load a table from the provided JDBCOptions",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "jdbcOptions",
          "required": false,
          "parameterType": "org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithJDBCOptions",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "72dbbfc8-bd1d-4ce4-ab35-28fa8385ea54",
      "displayName": "Load table as DataFrame using StepOptions",
      "description": "This step will load a table from the provided JDBCDataFrameReaderOptions",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "object",
          "name": "jDBCStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.JDBCDataFrameReaderOptions",
          "parameterType": "com.acxiom.pipeline.steps.JDBCDataFrameReaderOptions"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithStepOptions",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "dcc57409-eb91-48c0-975b-ca109ba30195",
      "displayName": "Load table as DataFrame",
      "description": "This step will load a table from the provided jdbc information",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "url",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "table",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "predicates",
          "required": false,
          "parameterType": "scala.Option[scala.List[String]]"
        },
        {
          "type": "text",
          "name": "connectionProperties",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithProperties",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "c9fddf52-34b1-4216-a049-10c33ccd24ab",
      "displayName": "Write DataFrame to table using JDBCOptions",
      "description": "This step will write a DataFrame as a table using JDBCOptions",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "jdbcOptions",
          "required": false,
          "parameterType": "org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions"
        },
        {
          "type": "text",
          "name": "saveMode",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.writeWithJDBCOptions",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "77ffcd02-fbd0-4f79-9b35-ac9dc5fb7190",
      "displayName": "Write DataFrame to table",
      "description": "This step will write a DataFrame to a table using the provided properties",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "url",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "table",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "connectionProperties",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        },
        {
          "type": "text",
          "name": "saveMode",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.writeWithProperties",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "3d6b77a1-52c2-49ba-99a0-7ec773dac696",
      "displayName": "Write DataFrame to JDBC table",
      "description": "This step will write a DataFrame to a table using the provided JDBCDataFrameWriterOptions",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "jDBCStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.JDBCDataFrameWriterOptions",
          "parameterType": "com.acxiom.pipeline.steps.JDBCDataFrameWriterOptions"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.writeWithStepOptions",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "219c787a-f502-4efc-b15d-5beeff661fc0",
      "displayName": "Map a DataFrame to an existing DataFrame",
      "description": "This step maps a new dataframe to an existing dataframe to make them compatible",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "destinationDataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations",
          "parameterType": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "boolean",
          "name": "addNewColumns",
          "required": false,
          "parameterType": "scala.Boolean"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapToDestinationDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "8f9c08ea-4882-4265-bac7-2da3e942758f",
      "displayName": "Map a DataFrame to a pre-defined Schema",
      "description": "This step maps a new dataframe to a pre-defined spark schema",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "destinationSchema",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Schema",
          "parameterType": "com.acxiom.pipeline.steps.Schema"
        },
        {
          "type": "object",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations",
          "parameterType": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "boolean",
          "name": "addNewColumns",
          "required": false,
          "parameterType": "scala.Boolean"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapDataFrameToSchema",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "3ee74590-9131-43e1-8ee8-ad320482a592",
      "displayName": "Merge a DataFrame to an existing DataFrame",
      "description": "This step merges two dataframes to create a single dataframe",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "destinationDataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations",
          "parameterType": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "boolean",
          "name": "addNewColumns",
          "required": false,
          "parameterType": "scala.Boolean"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mergeDataFrames",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "ac3dafe4-e6ee-45c9-8fc6-fa7f918cf4f2",
      "displayName": "Modify or Create Columns using Transforms Provided",
      "description": "This step transforms existing columns and/or adds new columns to an existing dataframe using expressions provided",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations",
          "parameterType": "com.acxiom.pipeline.steps.Transformations"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.applyTransforms",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "fa0fcabb-d000-4a5e-9144-692bca618ddb",
      "displayName": "Filter a DataFrame",
      "description": "This step will filter a dataframe based on the where expression provided",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "expression",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.applyFilter",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "a981080d-714c-4d36-8b09-d95842ec5655",
      "displayName": "Standardize Column Names on a DataFrame",
      "description": "This step will standardize columns names on existing dataframe",
      "type": "Pipeline",
      "category": "Transforms",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.standardizeColumnNames",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "541c4f7d-3524-4d53-bbd9-9f2cfd9d1bd1",
      "displayName": "Save a Dataframe to a TempView",
      "description": "This step stores an existing dataframe to a TempView to be used in future queries in the session",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "text",
          "name": "viewName",
          "required": false,
          "parameterType": "scala.Option[String]"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameToTempView",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "String"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "71b71ef3-eaa7-4a1f-b3f3-603a1a54846d",
      "displayName": "Create a TempView from a Query",
      "description": "This step runs a SQL statement against existing TempViews from this session and returns a new TempView",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql",
          "className": "String"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        },
        {
          "type": "text",
          "name": "viewName",
          "required": false,
          "parameterType": "scala.Option[String]"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.queryToTempView",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "String"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "61378ed6-8a4f-4e6d-9c92-6863c9503a54",
      "displayName": "Create a DataFrame from a Query",
      "description": "This step runs a SQL statement against existing TempViews from this session and returns a new DataFrame",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql",
          "className": "String"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.queryToDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "57b0e491-e09b-4428-aab2-cebe1f217eda",
      "displayName": "Create a DataFrame from an Existing TempView",
      "description": "This step pulls an existing TempView from this session into a new DataFrame",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "text",
          "name": "viewName",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.tempViewToDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "648f27aa-6e3b-44ed-a093-bc284783731b",
      "displayName": "Create a TempView from a DataFrame Query",
      "description": "This step runs a SQL statement against an existing DataFrame from this session and returns a new TempView",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql",
          "className": "String"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        },
        {
          "type": "text",
          "name": "inputViewName",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "outputViewName",
          "required": false,
          "parameterType": "scala.Option[String]"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameQueryToTempView",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "String"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "dfb8a387-6245-4b1c-ae6c-94067eb83962",
      "displayName": "Create a DataFrame from a DataFrame Query",
      "description": "This step runs a SQL statement against an existing DataFrame from this session and returns a new DataFrame",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql",
          "className": "String"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false,
          "parameterType": "scala.Option[Map[String,String]]"
        },
        {
          "type": "text",
          "name": "inputViewName",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameQueryToDataFrame",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "c88de095-14e0-4c67-8537-0325127e2bd2",
      "displayName": "Cache an exising TempView",
      "description": "This step will cache an existing TempView",
      "type": "Pipeline",
      "category": "Query",
      "params": [
        {
          "type": "text",
          "name": "viewName",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.cacheTempView",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrame"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "0342654c-2722-56fe-ba22-e342169545af",
      "displayName": "Copy source contents to destination",
      "description": "Copy the contents of the source path to the destination path. This function will call connect on both FileManagers.",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "srcFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "srcPath",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "destFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "destPath",
          "required": false,
          "parameterType": "String"
        }
      ],
      "engineMeta": {
        "spark": "FileManagerSteps.copy",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.steps.CopyResults"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "c40169a3-1e77-51ab-9e0a-3f24fb98beef",
      "displayName": "Copy source contents to destination with buffering",
      "description": "Copy the contents of the source path to the destination path using buffer sizes. This function will call connect on both FileManagers.",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "srcFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "srcPath",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "destFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "destPath",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "inputBufferSize",
          "required": false,
          "parameterType": "scala.Int"
        },
        {
          "type": "text",
          "name": "outputBufferSize",
          "required": false,
          "parameterType": "scala.Int"
        }
      ],
      "engineMeta": {
        "spark": "FileManagerSteps.copy",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.steps.CopyResults"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "f5a24db0-e91b-5c88-8e67-ab5cff09c883",
      "displayName": "Buffered file copy",
      "description": "Copy the contents of the source path to the destination path using full buffer sizes. This function will call connect on both FileManagers.",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "srcFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "srcPath",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "destFS",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        },
        {
          "type": "text",
          "name": "destPath",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "inputBufferSize",
          "required": false,
          "parameterType": "scala.Int"
        },
        {
          "type": "text",
          "name": "outputBufferSize",
          "required": false,
          "parameterType": "scala.Int"
        },
        {
          "type": "text",
          "name": "copyBufferSize",
          "required": false,
          "parameterType": "scala.Int"
        }
      ],
      "engineMeta": {
        "spark": "FileManagerSteps.copy",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.steps.CopyResults"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "3d1e8519-690c-55f0-bd05-1e7b97fb6633",
      "displayName": "Disconnect a FileManager",
      "description": "Disconnects a FileManager from the underlying file system",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "fileManager",
          "required": false,
          "parameterType": "com.acxiom.pipeline.fs.FileManager"
        }
      ],
      "engineMeta": {
        "spark": "FileManagerSteps.disconnectFileManager",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Unit"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "9d467cb0-8b3d-40a0-9ccd-9cf8c5b6cb38",
      "displayName": "Create SFTP FileManager",
      "description": "Simple function to generate the SFTPFileManager for the remote SFTP file system",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "hostName",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "username",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "password",
          "required": false,
          "parameterType": "String"
        },
        {
          "type": "text",
          "name": "port",
          "required": false,
          "parameterType": "scala.Int"
        },
        {
          "type": "text",
          "name": "strictHostChecking",
          "required": false,
          "parameterType": "scala.Option[scala.Boolean]"
        }
      ],
      "engineMeta": {
        "spark": "SFTPSteps.createFileManager",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "scala.Option[com.acxiom.pipeline.fs.SFTPFileManager]"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "22fcc0e7-0190-461c-a999-9116b77d5919",
      "displayName": "Build a DataFrameReader Object",
      "description": "This step will build a DataFrameReader object that can be used to read a file into a dataframe",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "object",
          "name": "dataFrameReaderOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.DataFrameReaderOptions",
          "parameterType": "com.acxiom.pipeline.steps.DataFrameReaderOptions"
        }
      ],
      "engineMeta": {
        "spark": "DataFrameSteps.getDataFrameReader",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrameReader"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "e023fc14-6cb7-44cb-afce-7de01d5cdf00",
      "displayName": "Build a DataFrameWriter Object",
      "description": "This step will build a DataFrameWriter object that can be used to write a file into a dataframe",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false,
          "parameterType": "org.apache.spark.sql.DataFrame"
        },
        {
          "type": "object",
          "name": "options",
          "required": false,
          "className": "com.acxiom.pipeline.steps.DataFrameWriterOptions",
          "parameterType": "com.acxiom.pipeline.steps.DataFrameWriterOptions"
        }
      ],
      "engineMeta": {
        "spark": "DataFrameSteps.getDataFrameWriter",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row]"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "5e0358a0-d567-5508-af61-c35a69286e4e",
      "displayName": "Javascript Step",
      "description": "Executes a script and returns the result",
      "type": "Pipeline",
      "category": "Scripting",
      "params": [
        {
          "type": "script",
          "name": "script",
          "required": false,
          "language": "javascript",
          "className": "String"
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScript",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.PipelineStepResponse"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    },
    {
      "id": "570c9a80-8bd1-5f0c-9ae0-605921fe51e2",
      "displayName": "Javascript Step with additional object provided",
      "description": "Executes a script and returns the result",
      "type": "Pipeline",
      "category": "Scripting",
      "params": [
        {
          "type": "script",
          "name": "script",
          "required": false,
          "language": "javascript",
          "className": "String"
        },
        {
          "type": "text",
          "name": "value",
          "required": false,
          "parameterType": "scala.Any"
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScriptWithValue",
        "pkg": "com.acxiom.pipeline.steps",
        "results": {
          "primaryType": "com.acxiom.pipeline.PipelineStepResponse"
        }
      },
      "tags": [
        "metalus-common_2.11-spark_2.3-1.5.0-SNAPSHOT.jar",
        "metalus-common_2.11-spark_2.4-1.5.0-SNAPSHOT.jar"
      ]
    }
  ],
  "pkgObjs": [
    {
      "id": "com.acxiom.pipeline.steps.JDBCDataFrameReaderOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"JDBC Data Frame Reader Options\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"url\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"},\"predicates\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"readerOptions\":{\"$ref\":\"#/definitions/DataFrameReaderOptions\"}},\"definitions\":{\"DataFrameReaderOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"options\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}},\"schema\":{\"$ref\":\"#/definitions/Schema\"}}},\"Schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/Attribute\"}}}},\"Attribute\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},\"dataType\":{\"$ref\":\"#/definitions/AttributeType\"}}},\"AttributeType\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"baseType\":{\"type\":\"string\"},\"valueType\":{\"$ref\":\"#/definitions/AttributeType\"},\"nameType\":{\"$ref\":\"#/definitions/AttributeType\"},\"schema\":{\"$ref\":\"#/definitions/Schema\"}}}}}"
    },
    {
      "id": "com.acxiom.pipeline.steps.DataFrameWriterOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Data Frame Writer Options\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"saveMode\":{\"type\":\"string\"},\"options\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}},\"bucketingOptions\":{\"$ref\":\"#/definitions/BucketingOptions\"},\"partitionBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"sortBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"definitions\":{\"BucketingOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"numBuckets\":{\"type\":\"integer\"},\"columns\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"numBuckets\"]}}}"
    },
    {
      "id": "com.acxiom.pipeline.steps.DataFrameReaderOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Data Frame Reader Options\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"options\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}},\"schema\":{\"$ref\":\"#/definitions/Schema\"}},\"definitions\":{\"Schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/Attribute\"}}}},\"Attribute\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},\"dataType\":{\"$ref\":\"#/definitions/AttributeType\"}}},\"AttributeType\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"baseType\":{\"type\":\"string\"},\"valueType\":{\"$ref\":\"#/definitions/AttributeType\"},\"nameType\":{\"$ref\":\"#/definitions/AttributeType\"},\"schema\":{\"$ref\":\"#/definitions/Schema\"}}}}}"
    },
    {
      "id": "com.acxiom.pipeline.steps.Transformations",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Transformations\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"columnDetails\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/ColumnDetails\"}},\"filter\":{\"type\":\"string\"},\"standardizeColumnNames\":{}},\"definitions\":{\"ColumnDetails\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"outputField\":{\"type\":\"string\"},\"inputAliases\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"expression\":{\"type\":\"string\"}}}}}"
    },
    {
      "id": "com.acxiom.pipeline.steps.Schema",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Schema\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/Attribute\"}}},\"definitions\":{\"Attribute\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},\"dataType\":{\"$ref\":\"#/definitions/AttributeType\"}}},\"AttributeType\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"baseType\":{\"type\":\"string\"},\"valueType\":{\"$ref\":\"#/definitions/AttributeType\"},\"nameType\":{\"$ref\":\"#/definitions/AttributeType\"},\"schema\":{\"$ref\":\"#/definitions/Schema\"}}},\"Schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"$ref\":\"#/definitions/Attribute\"}}}}}}"
    },
    {
      "id": "com.acxiom.pipeline.steps.JDBCDataFrameWriterOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"JDBC Data Frame Writer Options\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"url\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"},\"writerOptions\":{\"$ref\":\"#/definitions/DataFrameWriterOptions\"}},\"definitions\":{\"DataFrameWriterOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"saveMode\":{\"type\":\"string\"},\"options\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}},\"bucketingOptions\":{\"$ref\":\"#/definitions/BucketingOptions\"},\"partitionBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"sortBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}},\"BucketingOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"numBuckets\":{\"type\":\"integer\"},\"columns\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"numBuckets\"]}}}"
    }
  ]
}
```
