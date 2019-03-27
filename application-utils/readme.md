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
      "id": "0a296858-e8b7-43dd-9f55-88d00a7cd8fa",
      "displayName": "Write DataFrame to HDFS",
      "description": "This step will write a dataFrame in a given format to HDFS",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "dataFrame"
        },
        {
          "type": "text",
          "name": "path"
        },
        {
          "type": "text",
          "name": "format"
        },
        {
          "type": "text",
          "name": "saveMode"
        }
      ],
      "engineMeta": {
        "spark": "HDFSSteps.writeDataFrame"
      }
    },
    {
      "id": "cdb332e3-9ea4-4c96-8b29-c1d74287656c",
      "displayName": "Load table as DataFrame",
      "description": "This step will load a table from the provided jdbc information",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "jdbcOptions"
        },
        {
          "type": "text",
          "name": "columns"
        },
        {
          "type": "text",
          "name": "where"
        }
      ],
      "engineMeta": {
        "spark": "JDBCSteps.read"
      }
    },
    {
      "id": "219c787a-f502-4efc-b15d-5beeff661fc0",
      "displayName": "Map a DataFrame to an existing DataFrame",
      "description": "This step maps a new dataframe to an existing dataframe to make them compatible",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame"
        },
        {
          "type": "text",
          "name": "destinationDataFrame"
        },
        {
          "type": "text",
          "name": "transforms"
        },
        {
          "type": "boolean",
          "name": "addNewColumns"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapToDestinationDataFrame"
      }
    },
    {
      "id": "8f9c08ea-4882-4265-bac7-2da3e942758f",
      "displayName": "Map a DataFrame to a pre-defined Schema",
      "description": "This step maps a new dataframe to a pre-defined spark schema (StructType)",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame"
        },
        {
          "type": "text",
          "name": "destinationSchema"
        },
        {
          "type": "text",
          "name": "transforms"
        },
        {
          "type": "boolean",
          "name": "addNewColumns"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapDataFrameToSchema"
      }
    },
    {
      "id": "3ee74590-9131-43e1-8ee8-ad320482a592",
      "displayName": "Map a DataFrame to an existing DataFrame",
      "description": "This step maps a new dataframe to an existing dataframe to make them compatible",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "inputDataFrame"
        },
        {
          "type": "text",
          "name": "destinationDataFrame"
        },
        {
          "type": "text",
          "name": "transforms"
        },
        {
          "type": "boolean",
          "name": "addNewColumns"
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
      "params": [
        {
          "type": "text",
          "name": "dataFrame"
        },
        {
          "type": "text",
          "name": "transforms"
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
      "params": [
        {
          "type": "text",
          "name": "dataFrame"
        },
        {
          "type": "text",
          "name": "expression"
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
      "params": [
        {
          "type": "text",
          "name": "dataFrame"
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.standardizeColumnNames"
      }
    },
    {
      "id": "5e0358a0-d567-5508-af61-c35a69286e4e",
      "displayName": "Javascript Step",
      "description": "Executes a Javascript and returns the result",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "script"
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScript"
      }
    },
    {
      "id": "570c9a80-8bd1-5f0c-9ae0-605921fe51e2",
      "displayName": "Javascript Step with additional object provided",
      "description": "Executes a Javascript and returns the result",
      "type": "Pipeline",
      "params": [
        {
          "type": "text",
          "name": "script"
        },
        {
          "type": "text",
          "name": "value"
        }
      ],
      "engineMeta": {
        "spark": "JavascriptSteps.processScriptWithValue"
      }
    }
  ],
  "pkgObjs": [
    {
      "name": "com.acxiom.pipeline.steps.HiveStepsOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"format\":{\"type\":\"string\"},\"partitionBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"sortBy\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"bucketingOptions\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"numBuckets\":{\"type\":\"integer\"},\"columns\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"numBuckets\",\"columns\"]},\"options\":{\"type\":\"object\",\"patternProperties\":{\"^.*$\":{\"type\":\"string\"}}},\"saveMode\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"}},\"required\":[\"table\"]}"
    },
    {
      "name": "com.acxiom.pipeline.steps.JDBCStepsOptions",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"url\":{\"type\":\"string\"},\"table\":{\"type\":\"string\"},\"predicates\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"connectionProperties\":{\"type\":\"object\",\"patternProperties\":{\"^.*$\":{\"type\":\"string\"}}}},\"required\":[\"url\",\"table\"]}"
    },
    {
      "name": "com.acxiom.pipeline.steps.Transformations",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"columnDetails\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"outputField\":{\"type\":\"string\"},\"inputAliases\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"expression\":{\"type\":\"string\"}},\"required\":[\"outputField\"]}},\"filter\":{\"type\":\"string\"},\"standardizeColumnNames\":{\"type\":\"boolean\"}},\"required\":[\"columnDetails\"]}"
    },
    {
      "name": "com.acxiom.pipeline.steps.Schema",
      "schema": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"attributes\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},\"dataType\":{\"type\":\"string\"}},\"required\":[\"name\",\"dataType\"]}}},\"required\":[\"attributes\"]}"
    }
  ]
}
```

