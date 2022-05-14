[Documentation Home](readme.md)

# Application Example
This example will demonstrate how to create a Metalus Application and highlight several of the most powerful features. A
simple ETL process that will load a flat file containing orders data, credit card data, customer data and product data.
The first execution will be responsible for reading the data into a DataFrame which will be shared with the individual
executions for further processing.

The product, customer, credit card and orders executions will wait until the **ROOT** execution completes and then
execute in parallel if there are enough resources. 

Once complete, a final execution will write the data to a Mongo database.

[Parameter Mappings](parameter-mapping.md) will be used throughout this example to demonstrate the reusable nature of 
Pipelines. Several application parameters provided will be used for mapping. Since application parameters get 
automatically added to the _Globals_, the values can be accessed using the global mapping syntax:

|Command Line Parameter|Global Syntax|
|----------------|-------------------|
|input_url       |!input_url|
|input_format    |!input_format|
|input_separator |!input_separator|
|mongoURI        |!mongoURI|

Step mappings will also be used. Below is a table showing the mappings used in the first execution pipeline:

|Step Name|Step Syntax|
|----------------|-------------|
|READHEADERSTEP|@READHEADERSTEP|
|CREATESCHEMASTEP|@CREATESCHEMASTEP|

Note that the **@** syntax will automatically pull the _primaryReturn_ of the step response and not the entire step 
response.
## Example Code
All the code exists in this project as a way to quick start. This example will highlight and explain each of the features
including how to make a step.

The application-example.json configuration and orders.csv file are available in the 
[metalus-examples/mock_data](../metalus-examples/mock_data) directory. 

## Application configuration
Create a new file named *application-example.json* and place it somewhere that can be reached once the application 
starts.

The initial file should have a basic structure that has configuration for the *SparkConf*, *stepPackages*, *globals*,
and an empty *executions* array.
 
```json
{
  "sparkConf": {
    "kryoClasses": [
      "org.apache.hadoop.io.LongWritable",
      "org.apache.http.client.entity.UrlEncodedFormEntity"
    ],
    "setOptions": [
      {
        "name": "spark.hadoop.io.compression.codecs",
        "value": "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.GzipCodec"
      }
    ]
  },
  "stepPackages": [
    "com.acxiom.pipeline",
    "com.acxiom.pipeline.steps",
    "com.acxiom.metalus.steps.mongo"
  ],
  "globals": {
    "GlobalLinks": {
      "productDataFrame": "!PROD.pipelineParameters.b3171af2-618a-471e-937f-e1b4971e56cd.GROUPDATASTEP.primaryReturn",
      "customerDataFrame": "!CUST.pipelineParameters.53155d7a-6295-4e6c-83f4-d52ef3eabc5e.GROUPDATASTEP.primaryReturn",
      "creditCardDataFrame": "!CC.pipelineParameters.197b5aa0-3c87-481e-b335-8979f565b4b4.GROUPDATASTEP.primaryReturn",
      "orderDataFrame": "!ORD.pipelineParameters.c1722916-1c7f-46cd-bcff-ec9c4edb6b7f.GROUPDATASTEP.primaryReturn"
    }
  },
  "executions": []
}
```
The *globals* object contains a [GlobalLinks](executions.md#global-links) sub-object that contains *linked* global names 
referencing the dataframes that will be used by the other executions. 

The **inputFileDataFrame** dataframe will be used by the **PROD**, **CUST**, **CC** and **ORD** executions. The **GlobalLink**
definition allows the pipeline to reference **!inputFileDataFrame** instead of the [full path](executions.md#execution-results-syntax):
**!ROOT.pipelineParameters.f2dc5894-fe7d-4134-b0da-e5a3b8763a6e.LOADFILESTEP.primaryReturn**.

Additional **GlobalLinks** have been defined for the output of the **PROD**, **CUST**, **CC** and **ORD** executions 
which will be referenced in the **SAVE** execution.

## First Execution
The first execution will be responsible for the following actions:
* Parse header
* Create schema
* Load orders.csv

Three [steps](steps.md) will be created and added to the *InputOutputSteps* object. Each step will provide proper 
[annotations](step-annotations.md). Step annotations make it easier to [generate](metadata-extractor.md) the JSON metadata 
used in building [JSON pipelines](json-pipelines.md). 

### readHeader
The _readHeader_ step will only work with local files.

* Create a function named *readHeader* and declare three parameters:
	* url: String
	* format: String
	* separator: Option[String]
* Give the function a return type of List[String]
* Insert the following code into the body of the function (**Note**: This code is slightly different in the example project):

```scala
    val input = new FileInputStream(url)
    val head = Source.fromInputStream(input).getLines().next()
    input.close()
    head.split(separator.getOrElse(",")).map(_.toUpperCase).toList
```

Lastly, add the following annotations to the new function:
```scala
  @StepFunction("100b2c7d-c1fb-5fe2-b9d1-dd9fff103272",
    "Read header from a file",
    "This step will load the first line of a file and parse it into column names",
    "Pipeline",
    "Example")
  @StepParameters(Map("url" -> StepParameter(None, Some(true), None, None, description = Some("The file url")),
  "format" -> StepParameter(None, Some(true), None, None, description = Some("The file format")),
  "separator" -> StepParameter(None, Some(false), None, None, description = Some("The column separator"))))
```

### createSchema
* Create a function named *createSchema* and declare one parameter:
	* columnNames: List[String]
* Give the function a return type of StructType
* Insert the following code into the body of the function (**Note**: This code is slightly different in the example project):

```scala
    StructType(columnNames.map(StructField(_, StringType, nullable = true)))
```

Lastly, add the following annotations to the new function:
```scala
  @StepFunction("61f8c038-e632-5cad-b1c6-9da6034dce5c",
    "Create a DataFrame schema",
    "This step will create a DataFrame schema from a list of column names",
    "Pipeline",
    "Example")
  @StepParameters(Map("columnNames" -> StepParameter(None, Some(true), None, None, description = Some("The list of column names"))))
```

### loadFileWithSchema
* Create a function named *loadFileWithSchema* and declare five parameters:
	* url: String
	* format: String
	* separator: Option[String]
	* schema: Option[StructType]
	* pipelineContext: PipelineContext
* Give the function a return type of DataFrame
* Insert the following code into the body of the function (**Note**: This code is slightly different in the example project):

```scala
    val dfr = if (separator.isDefined) {
      pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head.toString)
    } else {
      pipelineContext.sparkSession.get.read.format(format)
    }

    val finalDF = if (schema.isEmpty) {
      dfr.load(url)
    } else {
      dfr.schema(schema.get).load(url)
    }

    PipelineStepResponse(Some(finalDF),
      Some(Map("$globalLink.inputFileDataFrame" ->
        "!ROOT.pipelineParameters.f2dc5894-fe7d-4134-b0da-e5a3b8763a6e.LOADFILESTEP.primaryReturn")))
```

Lastly, add the following annotations to the new function:
```scala
  @StepFunction("cba8a6d8-88b6-50ef-a073-afa6cba7ca1e",
    "Load File as Data Frame with schema",
    "This step will load a file from the provided URL using the provided schema",
    "Pipeline",
    "Example")
  @StepParameters(Map("url" -> StepParameter(None, Some(true), None, None, description = Some("The file url")),
    "format" -> StepParameter(None, Some(true), None, None, description = Some("The file format")),
    "separator" -> StepParameter(None, Some(false), None, None, description = Some("The column separator")),
    "schema" -> StepParameter(None, Some(false), None, None, description = Some("The optional schema to use"))))
```

**Note**: The existing *loadFile* function was refactored to call the new *loadFileWithSchema* function and pass
*None* as the schema parameter.

### JSON Pipeline
This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

f2dc5894-fe7d-4134-b0da-e5a3b8763a6e.json

Starting with the basic body:

```json
{
  "id": "f2dc5894-fe7d-4134-b0da-e5a3b8763a6e",
  "name": "Load Data Pipeline",
  "steps": []
}
```
#### Parse header
Create the step json:

```json
{
	"id": "READHEADERSTEP",
	"stepId": "100b2c7d-c1fb-5fe2-b9d1-dd9fff103272",
	"displayName": "Read header from a file",
	"description": "This step will load the first line of a file and parse it into column names",
	"type": "Pipeline",
	"nextStepId": "CREATESCHEMASTEP",
	"params": [
	  {
		"type": "string",
		"name": "url",
		"required": true,
		"value": "!input_url"
	  },
	  {
		"type": "string",
		"name": "format",
		"required": true,
		"value": "!input_format"
	  },
	  {
		"type": "string",
		"name": "separator",
		"required": true,
		"value": "!input_separator"
	  }
	],
	"engineMeta": {
	  "spark": "InputOutputSteps.readHeader"
	}
}
```
Add this as the first entry in the steps array.

#### Create schema
Create the step json:

```json
{
	"id": "CREATESCHEMASTEP",
	"stepId": "61f8c038-e632-5cad-b1c6-9da6034dce5c",
	"displayName": "Create a DataFrame schema",
	"description": "This step will create a DataFrame schema from a list of column names",
	"type": "Pipeline",
	"nextStepId": "LOADFILESTEP",
	"params": [
	  {
		"type": "list",
		"name": "columnNames",
		"required": true,
		"value": "@READHEADERSTEP"
	  }
	],
	"engineMeta": {
	  "spark": "InputOutputSteps.createSchema"
	}
}
```
Add this as the first entry in the steps array.

#### Load orders.csv
Create the step json:

```json
{
	"id": "LOADFILESTEP",
	"stepId": "cba8a6d8-88b6-50ef-a073-afa6cba7ca1e",
	"displayName": "Load File as Data Frame with schema",
	"description": "This step will load a file from the provided URL using the provided schema",
	"type": "Pipeline",
	"params": [
	  {
		"type": "string",
		"name": "url",
		"required": true,
		"value": "!input_url"
	  },
	  {
		"type": "string",
		"name": "format",
		"required": true,
		"value": "!input_format"
	  },
	  {
		"type": "string",
		"name": "separator",
		"required": true,
		"value": "!input_separator"
	  },
	  {
		"type": "text",
		"name": "schema",
		"required": true,
		"value": "@CREATESCHEMASTEP"
	  }
	],
	"engineMeta": {
	  "spark": "InputOutputSteps.loadFileWithSchema"
	}
}
```
Add this as the first entry in the steps array.

### Save the Pipeline JSON
Open the **application-example.json** and create an entry in the *executions* array.

```json
{
  "id": "ROOT",
  "pipelineIds": []
}
```
Save the pipeline id to the *pipelineIds* array.

## Extraction Executions (PROD, CUST, CC, ORD)
Each execution will run a very similar pipeline that with different parameter mappings. In order to reduce the amount of
duplication, a [step-group](step-groups.md) will be used. Each pipeline will use the **inputFileDataFrame GlobalLink** to
as the source *DataFrame* and perform various transformation tasks on the data. Each pipeline will generate a new 
*DataFrame* which has been mapped as a **GlobalLink** in the application configuration making it available to the final 
execution.

The details for the TransformationStep steps can be found in the [common-pipeline-steps](../metalus-common/readme.md) library.  
The Schema and Transformations objects passed to the steps should be added to the *Globals* section above of the Application 
(details provided in each extraction pipeline sections below).

### Adhere to Schema Step Group
A new JSON pipeline needs to be created that can be shared with the execution pipelines.

This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

07dc7c8e-7474-4b23-a108-0d8b551a404a.json

Starting with the basic body:

```json
{
  "id": "07dc7c8e-7474-4b23-a108-0d8b551a404a",
  "name": "Adhere DataFrame to Schema",
  "category": "step-group",
  "stepGroupResult": "@MAPFIELDSSTEP",
  "steps": []
}
```

The **category** specifies that this pipeline is a step-group and can be used inside other pipelines. The 
[stepGroupResult](step-groups.md#step-group-result) specifies the primary output value. This is an optional setting 
that allows the pipeline designer to specify which value should be presented to the caller. 

#### Map Fields Step
Create the step json:

```json
    {
      "id": "MAPFIELDSSTEP",
      "stepId": "8f9c08ea-4882-4265-bac7-2da3e942758f",
      "displayName": "Maps new data to a common schema",
      "description": "Creates a new DataFrame mapped to an existing schema",
      "type": "Pipeline",
      "params": [
        {
          "type": "string",
          "name": "inputDataFrame",
          "required": true,
          "value": "!dataFrame"
        },
        {
          "type": "string",
          "name": "destinationSchema",
          "required": true,
          "value": "!destinationSchema"
        },
        {
          "type": "string",
          "name": "transforms",
          "required": false,
          "value": "!destinationTransforms"
        },
        {
          "type": "boolean",
          "name": "addNewColumns",
          "required": true,
          "value": false
        }
      ],
      "engineMeta": {
        "spark": "TransformationSteps.mapDataFrameToSchema"
      }
    }
```
Add this as the only entry in the steps array.

### GroupingSteps
A new step will be created to perform grouping:

* Open the object in the *com.acxiom.pipeline.steps* package named [**GroupingSteps**](../metalus-examples/src/main/scala/com/acxiom/pipeline/steps/GroupingSteps.scala)
* Create a function named *groupByField* and declare two parameters:
	* dataFrame: DataFrame
	* groupField: String
* Give the function a return type of DataFrame
* Insert the following code into the body of the function:

```scala
    dataFrame.groupBy(dataFrame.schema.fields.map(field => dataFrame(field.name)): _*).agg(dataFrame(groupByField))
```

Lastly, add the following annotations to the new function:
```scala
  @StepFunction("99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
    "Performs a grouping and aggregation of the data",
    "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
    "Pipeline",
    "Example")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, description = Some("The DataFrame to group")),
    "groupByField" -> StepParameter(None, Some(true), None, None, description = Some("The column name to use for grouping"))))
```

### Extract Product Data Execution (PROD)
This execution will extract the product data from the initial DataFrame. The newly created step group will be used to
do extraction while the new step will be used to perform grouping.

This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

b3171af2-618a-471e-937f-e1b4971e56cd.json

JSON body:

```json
{
  "id": "b3171af2-618a-471e-937f-e1b4971e56cd",
  "name": "Extract Product Data Pipeline",
  "steps": [
    {
      "id": "MAPFIELDSSTEP",
      "stepId": "07dc7c8e-7474-4b23-a108-0d8b551a404a",
      "displayName": "Adhere DataFrame to Schema",
      "description": "Adhere DataFrame to Schema",
      "type": "step-group",
      "nextStepId": "GROUPDATASTEP",
      "params": [
        {
          "type": "string",
          "name": "pipelineId",
          "required": true,
          "value": "07dc7c8e-7474-4b23-a108-0d8b551a404a"
        },
        {
          "type": "object",
          "name": "pipelineMappings",
          "required": true,
          "value": {
            "dataFrame": "!inputFileDataFrame",
            "destinationSchema": {
              "className": "com.acxiom.pipeline.steps.Schema",
              "value": {
                "attributes": [
                  {
                    "name": "PRODUCT_NAME",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "PRODUCT_ID",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "COST",
                    "dataType": {
                      "baseType": "Double"
                    }
                  }
                ]
              }
            }
          }
        }
      ]
    },
    {
      "id": "GROUPDATASTEP",
      "stepId": "99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
      "displayName": "Performs a grouping and aggregation of the data",
      "description": "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
      "type": "Pipeline",
      "params": [
        {
          "type": "string",
          "name": "dataFrame",
          "required": true,
          "value": "@MAPFIELDSSTEP"
        },
        {
          "type": "string",
          "name": "groupByField",
          "required": true,
          "value": "PRODUCT_ID"
        }
      ],
      "engineMeta": {
        "spark": "GroupingSteps.groupByField"
      }
    }
  ]
}
```

This first step defines the step group created earlier with two parameters:
* **pipelineId** - The id of the step group pipeline
* **pipelineMappings** - The custom values to be processed by the step-group pipeline. 
    * _dataFrame_ - This is the DataFrame created by the root execution
    * _destinationSchema_ - This example builds out the schema object using [JSON object](parameter-mapping.md#json-objects) 
    syntax. which will be responsible for setting the column order, column names, and data types on output. The _transforms_ 
    parameter will not be provided since no aliases or transformations will be used for this pipeline.

The execution will be added in the _executions_ array after the initial execution
```json
{
  "id": "PROD",
  "pipelineIds": [
],
  "parents": [
    "ROOT"
  ]
}
```

### Extract Customer Data Pipeline (CUST)
This execution will extract the customer data from the initial DataFrame. The newly created step group will be used to
do extraction while the new step will be used to perform grouping.

This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

53155d7a-6295-4e6c-83f4-d52ef3eabc5e.json

JSON body:

```json
{
  "id": "53155d7a-6295-4e6c-83f4-d52ef3eabc5e",
  "name": "Extract Customer Data Pipeline",
  "steps": [
    {
      "id": "MAPFIELDSSTEP",
      "stepId": "07dc7c8e-7474-4b23-a108-0d8b551a404a",
      "displayName": "Adhere DataFrame to Schema",
      "description": "Adhere DataFrame to Schema",
      "type": "step-group",
      "nextStepId": "GROUPDATASTEP",
      "params": [
        {
          "type": "string",
          "name": "pipelineId",
          "required": true,
          "value": "07dc7c8e-7474-4b23-a108-0d8b551a404a"
        },
        {
          "type": "object",
          "name": "pipelineMappings",
          "required": true,
          "value": {
            "dataFrame": "!inputFileDataFrame",
            "destinationSchema": {
              "className": "com.acxiom.pipeline.steps.Schema",
              "value": {
                "attributes": [
                  {
                    "name": "CUSTOMER_ID",
                    "dataType": {
                      "baseType": "Integer"
                    }
                  },
                  {
                    "name": "FIRST_NAME",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "LAST_NAME",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "POSTAL_CODE",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "GENDER_CODE",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "EIN",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "EMAIL",
                    "dataType": {
                      "baseType": "String"
                    }
                  },
                  {
                    "name": "FULL_NAME",
                    "dataType": {
                      "baseType": "STRING"
                    }
                  }
                ]
              }
            },
            "destinationTransforms": {
              "className": "com.acxiom.pipeline.steps.Transformations",
              "value": {
                "columnDetails": [
                  {
                    "outputField": "GENDER_CODE",
                    "inputAliases": ["GENDER"],
                    "expression": "upper(substring(GENDER_CODE,0,1))"
                  },
                  {
                    "outputField": "FULL_NAME",
                    "inputAliases": [],
                    "expression": "concat(initcap(FIRST_NAME), ' ', initcap(LAST_NAME))"
                  }
                ]
              }
            }
          }
        }
      ]
    },
    {
      "id": "GROUPDATASTEP",
      "stepId": "99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
      "displayName": "Performs a grouping and aggregation of the data",
      "description": "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
      "type": "Pipeline",
      "params": [
        {
          "type": "string",
          "name": "dataFrame",
          "required": true,
          "value": "@MAPFIELDSSTEP"
        },
        {
          "type": "string",
          "name": "groupByField",
          "required": true,
          "value": "CUSTOMER_ID"
        }
      ],
      "engineMeta": {
        "spark": "GroupingSteps.groupByField"
      }
    }
  ]
}
```

This first step defines the step group created earlier with two parameters:
* **pipelineId** - The id of the step group pipeline
* **pipelineMappings** - The custom values to be processed by the step-group pipeline. 
    * _dataFrame_ - This is the DataFrame created by the root execution
    * _destinationSchema_ - This example builds out the schema object using [JSON object](parameter-mapping.md#json-objects) 
    syntax. which will be responsible for setting the column order, column names, and data types on output.
    * _destinationTransforms_ - This example builds out a transformation object that will rename GENDER to GENDER_CODE 
    applying logic to only save the first character in upper case and adding a new field called FULL_NAME which is built 
    from concatenating first name to last name.

The execution will be added in the _executions_ array after the _PROD_ execution
```json
    {
      "id": "CUST",
      "pipelineIds": [
        "53155d7a-6295-4e6c-83f4-d52ef3eabc5e"
      ],
      "parents": [
        "ROOT"
      ]
    }
```
### Extract Credit Card Data Pipeline (CC)
This execution will extract the credit card data from the initial DataFrame. The newly created step group will be used to
do extraction while the new step will be used to perform grouping.

The following parameters should be added to the application globals which will be responsible for setting the column order,
column names, and data types on output.  Specifically, renaming CC_NUM to ACCOUNT_NUMBER, CC_TYPE to ACCOUNT_TYPE, and converting
ACCOUNT_TYPE to uppercase:
```json
"creditCardSchema": {
      "className": "com.acxiom.pipeline.steps.Schema",
      "object": {
        "attributes": [
          {
            "name": "CUSTOMER_ID",
            "dataType": "Integer"
          },
          {
            "name": "ACCOUNT_NUMBER",
            "dataType": "String"
          },
          {
            "name": "ACCOUNT_TYPE",
            "dataType": "String"
          }
        ]
      }
    },
    "creditCardTransforms": {
      "className": "com.acxiom.pipeline.steps.Transformations",
      "object": {
        "columnDetails": [
          {
            "outputField": "ACCOUNT_NUMBER",
            "inputAliases": ["CC_NUM"],
            "expression": null
          },
          {
            "outputField": "ACCOUNT_TYPE",
            "inputAliases": ["CC_TYPE"],
            "expression": "upper(ACCOUNT_TYPE)"
          }
        ]
      }
    }
```

This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

197b5aa0-3c87-481e-b335-8979f565b4b4.json

JSON body:

```json
{
  "id": "197b5aa0-3c87-481e-b335-8979f565b4b4",
  "name": "Extract Credit Card Data Pipeline",
  "steps": [
    {
      "id": "MAPFIELDSSTEP",
      "stepId": "07dc7c8e-7474-4b23-a108-0d8b551a404a",
      "displayName": "Adhere DataFrame to Schema",
      "description": "Adhere DataFrame to Schema",
      "type": "step-group",
      "nextStepId": "GROUPDATASTEP",
      "params": [
        {
          "type": "string",
          "name": "pipelineId",
          "required": true,
          "value": "07dc7c8e-7474-4b23-a108-0d8b551a404a"
        },
        {
          "type": "object",
          "name": "pipelineMappings",
          "required": true,
          "value": {
            "dataFrame": "!inputFileDataFrame",
            "destinationSchema": "!creditCardSchema",
            "destinationTransforms": "!creditCardTransforms"
          }
        }
      ]
    },
    {
      "id": "GROUPDATASTEP",
      "stepId": "99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
      "displayName": "Performs a grouping and aggregation of the data",
      "description": "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
      "type": "Pipeline",
      "params": [
        {
          "type": "string",
          "name": "dataFrame",
          "required": true,
          "value": "@MAPFIELDSSTEP"
        },
        {
          "type": "string",
          "name": "groupByField",
          "required": true,
          "value": "CUSTOMER_ID"
        }
      ],
      "engineMeta": {
        "spark": "GroupingSteps.groupByField"
      }
    }
  ]
}

```

This first step defines the step group created earlier with two parameters:
* **pipelineId** - The id of the step group pipeline
* **pipelineMappings** - The custom values to be processed by the step-group pipeline. 
    * _dataFrame_ - This is the DataFrame created by the root execution
    * _destinationSchema_ - This example uses the global defined earlier.
    * _destinationTransforms_ - This example uses the global defined earlier.

The execution will be added in the _executions_ array after the _CUST_ execution
```json
    {
      "id": "CC",
      "pipelineIds": [
        "197b5aa0-3c87-481e-b335-8979f565b4b4"
      ],
      "parents": [
        "ROOT"
      ]
    }
```
### Extract Order Data Pipeline (ORD)
This execution will extract the order data from the initial DataFrame. The newly created step group will be used to
do extraction while the new step will be used to perform grouping.

The following parameters should be added to the application globals which will be responsible for setting the column order,
column names, and data types on output. Specifically, the ORDER_NUM field will be renamed to ORDER_ID: 
```json
"orderSchema": {
      "className": "com.acxiom.pipeline.steps.Schema",
      "object": {
        "attributes": [
          {
            "name": "ORDER_ID",
            "dataType": "String"
          },
          {
            "name": "CUSTOMER_ID",
            "dataType": "Integer"
          },
          {
            "name": "PRODUCT_ID",
            "dataType": "String"
          },
          {
            "name": "UNITS",
            "dataType": "Integer"
          }
        ]
      }
    },
    "orderTransforms": {
      "className": "com.acxiom.pipeline.steps.Transformations",
      "object": {
        "columnDetails": [
          {
            "outputField": "ORDER_ID",
            "inputAliases": ["ORDER_NUM"],
            "expression": null
          }
        ]
      }
    }
```

This pipeline will be stored with the [step library](step-libraries.md#pipelines) in a JSON file named:

c1722916-1c7f-46cd-bcff-ec9c4edb6b7f.json

JSON body:

```json
{
  "id": "c1722916-1c7f-46cd-bcff-ec9c4edb6b7f",
  "name": "Extract Order Data Pipeline",
  "steps": [
    {
      "id": "MAPFIELDSSTEP",
      "stepId": "07dc7c8e-7474-4b23-a108-0d8b551a404a",
      "displayName": "Adhere DataFrame to Schema",
      "description": "Adhere DataFrame to Schema",
      "type": "step-group",
      "nextStepId": "GROUPDATASTEP",
      "params": [
        {
          "type": "string",
          "name": "pipelineId",
          "required": true,
          "value": "07dc7c8e-7474-4b23-a108-0d8b551a404a"
        },
        {
          "type": "object",
          "name": "pipelineMappings",
          "required": true,
          "value": {
            "dataFrame": "!inputFileDataFrame",
            "destinationSchema": "!creditCardSchema",
            "destinationTransforms": "!orderTransforms"
          }
        }
      ]
    },
    {
      "id": "GROUPDATASTEP",
      "stepId": "99ad5ed4-b907-5635-8f2a-1c9012f6f5a7",
      "displayName": "Performs a grouping and aggregation of the data",
      "description": "Performs a grouping across all columns in the DataFrame and aggregation using the groupByField of the data.",
      "type": "Pipeline",
      "params": [
        {
          "type": "string",
          "name": "dataFrame",
          "required": true,
          "value": "@MAPFIELDSSTEP"
        },
        {
          "type": "string",
          "name": "groupByField",
          "required": true,
          "value": "ORDER_ID"
        }
      ],
      "engineMeta": {
        "spark": "GroupingSteps.groupByField"
      }
    }
  ]
}
```

This first step defines the step group created earlier with two parameters:
* **pipelineId** - The id of the step group pipeline
* **pipelineMappings** - The custom values to be processed by the step-group pipeline. 
    * _dataFrame_ - This is the DataFrame created by the root execution
    * _destinationSchema_ - This example uses the global defined earlier.
    * _destinationTransforms_ - This example uses the global defined earlier.

The execution will be added in the _executions_ array after the _CC_ execution
```json
    {
      "id": "ORD",
      "pipelineIds": [
        "c1722916-1c7f-46cd-bcff-ec9c4edb6b7f"
      ],
      "parents": [
        "ROOT"
      ]
    }
```
## Final Execution
Once the data has been loaded and processed into different forms, a final execution will be responsible for writing 
the data to a Mongo data store. This pipeline is only here to demonstrate the [fork](fork-join.md) step for iterating over
a list of values using the same steps. This example demonstrates how to wait on all the transformation executions 
completing before continuing to write.

The [metalus-mongo](../metalus-mongo/readme.md) step library will be required as well as the mongo spark connector jars.

Instead of storing the pipeline within a JSON file in a step library, this pipeline will be stored in the _pipelines_ 
array in the application configuration after the globals declaration.

The first step is the [fork](fork-join.md) step used to iterate over the list provided by the _forkByValues_ parameter. An
inline [scalascript](step-templates.md#scala-script-parameters) is used to generate a list of tuples that will be used 
for iteration. The _forkMethod_ is set to parallel to allow Metalus to attempt to process each DataFrame at the same time.
```json
{
  "pipelines": [
    {
      "id": "470ffe46-5162-43e0-9ae9-ea9205efe256",
      "name": "Write Data Pipeline",
      "steps": [
        {
          "id": "FORKSTEP",
          "type": "fork",
          "nextStepId": "SAVEDATA",
          "params": [
            {
              "type": "scalascript",
              "name": "forkByValues",
              "required": true,
              "value": "(prodDF:!productDataFrame:org.apache.spark.sql.DataFrame,custDF:!customerDataFrame,ccDF:!creditCardDataFrame,ordDF:!orderDataFrame:) List((\"products\", prodDF), (\"customers\", custDF), (\"creditCards\", ccDF), (\"orders\", ordDF))"
            },
            {
              "type": "text",
              "name": "forkMethod",
              "value": "serial"
            }
          ]
        },
        {
          "id": "SAVEDATA",
          "type": "Pipeline",
          "params": [
            {
              "type": "text",
              "name": "dataFrame",
              "required": true,
              "value": "@FORKSTEP._2"
            },
            {
              "type": "text",
              "name": "connector",
              "required": true,
              "value": "!mongoConnector"
            },
            {
              "type": "string",
              "name": "destination",
              "required": true,
              "value": "@FORKSTEP._1"
            }
          ],
          "engineMeta": {
            "spark": "DataConnectorSteps.writeDataFrame"
          }
        }
      ]
    }
  ]
}
```
The [scalascript](step-templates.md#scala-script-parameters) mapping was added to demonstrate the ability to dynamically 
build mappings. This prevents the need for unnecessary steps to massage data into a format that a step requires. A script
may also have data mapped into the parameters. Notice how the first parameter uses the **Global** syntax to access the
DataFrame produced by the **PROD** execution. Note that it is referencing the **GlobalLink**. Any mapping syntax can 
be used except embedded _scalascript_.

```scala
(prodDF:!productDataFrame:org.apache.spark.sql.DataFrame,custDF:!customerDataFrame,ccDF:!creditCardDataFrame,ordDF:!orderDataFrame:) List((\"products\", prodDF), (\"customers\", custDF), (\"creditCards\", ccDF), (\"orders\", ordDF))
``` 

This simple mapping could have been accomplished with a map.

The save step uses the [MongoDataConnector](dataconnectors.md#mongodataconnector) to handle the save action. The connector
is reused for each collection allowing each fork to specify the collection name. The following should e added to the globals:

```json
"mongoConnector": {
  "mapEmbeddedVariables": true,
  "className": "com.acxiom.metalus.pipeline.connectors.MongoDataConnector",
  "object": {
    "name": "MongoConnector",
    "uri": "!mongoURI"
  }
}
```
Notice the _uri_ attribute is set to _!mongoURI_. This will allow the command line parameter to be mapped to the connector 
when the application loads. The object uses the [mapEmbeddedVariables](applications.md#objects) attribute which indicates 
that this object should have embedded variables mapped.
### Final Execution JSON
```json
{
  "id": "SAVE",
  "pipelineIds": [
	"470ffe46-5162-43e0-9ae9-ea9205efe256"
  ],
  "parents": [
	"PROD",
	"CUST",
	"CC",
	"ORD"
  ]
}
```
### Use existing DriverSetup
Since the configuration is completely JSON based, there is no need to create a new *DriverSetup*. The 
*com.acxiom.pipeline.applications.DefaultApplicationDriverSetup* is available to configure the execution plan using the
JSON configuration that has been built. 

## Running
An [application jar](../metalus-application/readme.md) is provided for the main jar and the 
metalus-common, metalus-mongo and metalus-examples jars provided to the *--jars* parameter.

The application commands below provide the proper templates to run the example:

* _\<VERSION>_ - The current Metalus version
* _<jar_path>_ - The fully qualified path to the built jars
* _<data_location>_ - The fully qualified path to the example data

### Spark 2.4/Scala 2.11
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.4-<VERSION>.jar,metalus-examples_2.11-spark_2.4-<VERSION>.jar,metalus-mongo_2.11-spark_2.4-<VERSION>.jar,mongo-spark-connector_2.11-2.4.1.jar,mongo-java-driver-3.11.2.jar  \
<jar_path>/metalus-application_2.11-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <data_location>/application-example.json \
--input_url <data_location>/orders.csv \
--input_format csv \
--input_separator , \
--mongoURI mongodb://localhost:27017/application_examples \
--logLevel DEBUG
```
### Spark 3.0/Scala 2.12
```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.12-spark_3.0-<VERSION>.jar,metalus-examples_2.12-spark_3.0-<VERSION>.jar,metalus-mongo_2.12-spark_3.0-<VERSION>.jar,mongo-spark-connector_2.12-3.0.0.jar,mongodb-driver-sync-4.0.5.jar,mongodb-driver-core-4.0.5.jar,bson-4.0.5.jar  \
<jar_path>/metalus-application_2.12-spark_3.0-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.applications.DefaultApplicationDriverSetup \
--applicationConfigPath <data_location>/application-example.json \
--input_url <data_location>/orders.csv \
--input_format csv \
--input_separator , \
--mongoURI mongodb://localhost:27017/application_examples \
--logLevel DEBUG
```
