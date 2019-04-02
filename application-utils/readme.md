# Application Utilities
Application utilities are provided as a way to make working with the project easier.

## Step Metadata Extractor
This utility will scan jar files that contain steps and produce a JSON representation. The tool takes a list of packages
and identifies objects with the annotation *StepObject*. Next the tool will look for any function annotated with the
*StepFunction* annotation. The output will contain the *pkgs* array with all of the packages that contained steps, the
*steps* array that contains the metadata generated for each step, and the *pkgObjs* array that contains schemas for all 
of the case classes used as parameters in each step.

### Running
The script parameters are:
* --jar-files - A comma separated list of jar files. This should be the full path.
* --step-packages A comma separated list of packages to scan. *Example: com.acxiom.pipeline.steps*
* --output-file - An optional file name to write the step metadata. The output will be written to the console otherwise.

Installation:
* Download the tar file from the releases page
* Expand the tar file (tar xzf application-utilities_2.11...)
* Change to the bin directory (cd application-utilities/bin)
* Run the command:

```bash
./step-metadata-extractor.sh --jar-files /tmp/steps.jar,/tmp/common-steps.jar --step-packages com.acxiom.pipeline.steps,com.mycompany.steps --output-file steps.json
```

### Example using common steps

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
          "name": "hiveStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.HiveStepsOptions"
        }
      ],
      "engineMeta": {
        "spark": "HiveSteps.readDataFrame"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "hiveStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.HiveStepsOptions"
        }
      ],
      "engineMeta": {
        "spark": "HiveSteps.writeDataFrame"
      }
    },
    {
      "id": "87db259d-606e-46eb-b723-82923349640f",
      "displayName": "Load DataFrame from HDFS",
      "description": "This step will create a dataFrame in a given format from HDFS",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "path",
          "required": false
        },
        {
          "type": "text",
          "name": "format",
          "required": false,
          "defaultValue": "parquet"
        },
        {
          "type": "text",
          "name": "properties",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "HDFSSteps.readFromHDFS"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "path",
          "required": false
        },
        {
          "type": "text",
          "name": "format",
          "required": false,
          "defaultValue": "parquet"
        },
        {
          "type": "text",
          "name": "properties",
          "required": false
        },
        {
          "type": "text",
          "name": "saveMode",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "HDFSSteps.writeDataFrame"
      }
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
          "language": "scala"
        }
      ],
      "engineMeta": {
        "spark": "ScalaSteps.processScript"
      }
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
          "language": "scala"
        },
        {
          "type": "text",
          "name": "value",
          "required": false
        },
        {
          "type": "text",
          "name": "type",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "ScalaSteps.processScriptWithValue"
      }
    },
    {
      "id": "cdb332e3-9ea4-4c96-8b29-c1d74287656c",
      "displayName": "Load table as DataFrame",
      "description": "This step will load a table from the provided jdbc information",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "jdbcOptions",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithJDBCOptions"
      }
    },
    {
      "id": "72dbbfc8-bd1d-4ce4-ab35-28fa8385ea54",
      "displayName": "Load JDBC table as DataFrame",
      "description": "This step will load a table from the provided jdbc step options",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "jDBCStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.JDBCStepsOptions"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithStepOptions"
      }
    },
    {
      "id": "dcc57409-eb91-48c0-975b-ca109ba30195",
      "displayName": "Load JDBC table as DataFrame with Properties",
      "description": "This step will load a table from the provided jdbc information",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "url",
          "required": false
        },
        {
          "type": "text",
          "name": "table",
          "required": false
        },
        {
          "type": "text",
          "name": "predicates",
          "required": false
        },
        {
          "type": "text",
          "name": "connectionProperties",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.readWithProperties"
      }
    },
    {
      "id": "c9fddf52-34b1-4216-a049-10c33ccd24ab",
      "displayName": "Write DataFrame to JDBC table",
      "description": "This step will write a DataFrame as a table using JDBC",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false
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
        "spark": "JDBCSteps.writeWithJDBCOptions"
      }
    },
    {
      "id": "77ffcd02-fbd0-4f79-9b35-ac9dc5fb7190",
      "displayName": "Write DataFrame to JDBC table with Properties",
      "description": "This step will write a DataFrame as a table using JDBC and provided properties",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false
        },
        {
          "type": "text",
          "name": "url",
          "required": false
        },
        {
          "type": "text",
          "name": "table",
          "required": false
        },
        {
          "type": "text",
          "name": "connectionProperties",
          "required": false
        },
        {
          "type": "text",
          "name": "saveMode",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.writeWithProperties"
      }
    },
    {
      "id": "3d6b77a1-52c2-49ba-99a0-7ec773dac696",
      "displayName": "Write DataFrame to JDBC table",
      "description": "This step will write a DataFrame as a table using JDBC using JDBCStepOptions",
      "type": "Pipeline",
      "category": "InputOutput",
      "params": [
        {
          "type": "text",
          "name": "dataFrame",
          "required": false
        },
        {
          "type": "text",
          "name": "jDBCStepsOptions",
          "required": false,
          "className": "com.acxiom.pipeline.steps.JDBCStepsOptions"
        },
        {
          "type": "text",
          "name": "saveMode",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.writeWithStepOptions"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "destinationDataFrame",
          "required": false
        },
        {
          "type": "text",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "text",
          "name": "addNewColumns",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapToDestinationDataFrame"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "destinationSchema",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Schema"
        },
        {
          "type": "text",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "text",
          "name": "addNewColumns",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapDataFrameToSchema"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "destinationDataFrame",
          "required": false
        },
        {
          "type": "text",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations"
        },
        {
          "type": "text",
          "name": "addNewColumns",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mergeDataFrames"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "transforms",
          "required": false,
          "className": "com.acxiom.pipeline.steps.Transformations"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.applyTransforms"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "expression",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.applyFilter"
      }
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
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.standardizeColumnNames"
      }
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
          "required": false
        },
        {
          "type": "text",
          "name": "viewName",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameToTempView"
      }
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
          "language": "sql"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false
        },
        {
          "type": "text",
          "name": "viewName",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.queryToTempView"
      }
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
          "language": "sql"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.queryToDataFrame"
      }
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
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.tempViewToDataFrame"
      }
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
          "required": false
        },
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false
        },
        {
          "type": "text",
          "name": "inputViewName",
          "required": false
        },
        {
          "type": "text",
          "name": "outputViewName",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameQueryToTempView"
      }
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
          "required": false
        },
        {
          "type": "script",
          "name": "query",
          "required": false,
          "language": "sql"
        },
        {
          "type": "text",
          "name": "variableMap",
          "required": false
        },
        {
          "type": "text",
          "name": "inputViewName",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.dataFrameQueryToDataFrame"
      }
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
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "QuerySteps.cacheTempView"
      }
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
          "language": "javascript"
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScript"
      }
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
          "language": "javascript"
        },
        {
          "type": "text",
          "name": "value",
          "required": false
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScriptWithValue"
      }
    }
  ],
  "pkgObjs": [
    {
      "id": "com.acxiom.pipeline.steps.HiveStepsOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"partitionBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"sortBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"bucketingOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"numBuckets\":{\"type\":\"integer\"},\"columns\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"numBuckets\",\"columns\"]},\"options\":{\"type\":\"object\",\"patternProperties\":{\"^.*$\":{\"type\":\"string\"}}},\"saveMode\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"}},\"required\":[\"table\"]}"
    },
    {
      "id": "com.acxiom.pipeline.steps.JDBCStepsOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"url\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"},\"predicates\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"connectionProperties\":{\"type\":\"object\",\"patternProperties\":{\"^.*$\":{\"type\":\"string\"}}}},\"required\":[\"url\",\"table\"]}"
    },
    {
      "id": "com.acxiom.pipeline.steps.Transformations",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"columnDetails\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"outputField\":{\"type\":\"string\"},\"inputAliases\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"expression\":{\"type\":\"string\"}},\"required\":[\"outputField\"]}},\"filter\":{\"type\":\"string\"},\"standardizeColumnNames\":{\"type\":\"boolean\"}},\"required\":[\"columnDetails\"]}"
    },
    {
      "id": "com.acxiom.pipeline.steps.Schema",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},\"dataType\":{\"type\":\"string\"}},\"required\":[\"name\",\"dataType\"]}}},\"required\":[\"attributes\"]}"
    }
  ]
}
```

