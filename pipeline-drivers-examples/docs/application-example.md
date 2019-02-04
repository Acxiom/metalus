# Application Example
This example will demonstrate how to use an execution plan to create pipeline dependencies as well
as execute pipelines in parallel.

There will be six pipelines that are executed:
* **ROOT** - This is the first pipeline and is responsible for reading in the source file.
* **PROD** - This pipeline will extract the product data from the **ROOT** DataFrame and group by the product id.
* **CUST** - This pipeline will extract the customer data from the **ROOT** DataFrame and group by the customer id.
* **CC** - This pipeline will extract the credit card data from the **ROOT** DataFrame and group by the customer id.
* **ORD** - This pipeline will extract the order data from the **ROOT** DataFrame and group by the order num.
* **SAVE** - This pipeline will write the data from the **PROD**, **CUST**, **CC** and **ORD** DataFrames into Mongo.

## Example Code
All of the code exists in this project as a way to quick start, however below is a walk through of creating each of the 
required components.

The data file has been added in the *mock_data* directory.

## Application configuration
Create a new file named *application-example.json* and place it somewhere that can be reached once the application 
starts. The example json file exists in the *mock_data* directory of this project as a reference.

The initial file should have a basic structure that has configuration for the *SparkConf*, *stepPackages* and an empty 
*executions* array.

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
    "com.acxiom.pipeline.steps",
    "com.acxiom.pipeline"
  ],
  "executions": []
}
```

## First Pipeline
The first pipeline will be responsible for the following actions:
* Parse header
* Create schema
* Load orders.csv

Three new steps are required to perform this process and will be added to the *InputOutputSteps* object:

**Note**: The readHeader step will only work with local files.

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

* Create a function named *createSchema* and declare one parameter:
	* columnNames: List[String]
* Give the function a return type of StructType
* Insert the following code into the body of the function (**Note**: This code is slightly different in the example project):

```scala
StructType(columnNames.map(StructField(_, StringType, nullable = true)))
```

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
  pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head)
} else {
  pipelineContext.sparkSession.get.read.format(format)
}

if (schema.isEmpty) {
  dfr.load(url)
} else {
  dfr.schema(schema.get).load(url)
}
```

**Note**: The existing *loadFile* function was refactored to call the new *loadFileWithSchema* function and pass
*None* as the schema parameter.

### JSON
Starting with the basic body:

```json
{
  "id": "LOAD_DATA_PIPELINE",
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
		"type": "string",
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
  "pipelines": []
}
```
Save the pipeline json to the *pipelines* array.

## Extraction Pipelines
Additional pipelines will be created that take the *DataFrame* generated in the *ROOT* execution
(available as a global lookup) and extract specific fields of data. Each pipeline will generate 
a new *DataFrame* which will be added to the globals object of the final execution.

Two new steps are required to perform this process:

* Create a new object in the *com.acxiom.pipeline.steps* package named [**MappingSteps**](src/main/scala/com/acxiom/pipeline/steps/MappingSteps.scala)
* Create a function named *selectFields* and declare two parameters:
	* dataFrame: DataFrame
	* fieldNames: List[String]
* Give the function a return type of DataFrame
* Create the function below:

```scala
def selectFields(dataFrame: DataFrame, fieldNames: List[String]): DataFrame =
			dataFrame.select(fieldNames.map(dataFrame(_)) : _*)
```

* Open the object in the *com.acxiom.pipeline.steps* package named [**GroupingSteps**](src/main/scala/com/acxiom/pipeline/steps/GroupingSteps.scala)
* Create a function named *groupByField* and declare two parameters:
	* dataFrame: DataFrame
	* groupField: String
* Give the function a return type of DataFrame
* Insert the following code into the body of the function:

```scala
dataFrame.groupBy(dataFrame.schema.fields.map(field => dataFrame(field.name)): _*).agg(dataFrame(groupByField))
```

Four pipelines will need to be created and stored in the **execution-pipelines.json** file. **Note**: That these four 
pipelines use the exact same steps except that some of the parameters are different. The **DriverSetup** could be used 
to optimize the data.

### Extract Product Data Execution (PROD)
This pipeline will take the *DataFrame* loaded in the first execution pipeline and use it as a parameter for the first 
step in the pipeline. The *MAPFIELDSSTEP* relies on the execution id being **ROOT**. This pipeline needs to be part of 
an execution that is dependent on the *ROOT* execution created previously.

```json
{
  "id": "PROD",
  "pipelines": [
	{
	  "id": "EXTRACT_PRODUCT_DATA_PIPELINE",
	  "name": "Extract Product Data Pipeline",
	  "steps": [
		{
		  "id": "MAPFIELDSSTEP",
		  "stepId": "772912d6-ee6a-5228-ae7a-0127eb2dce37",
		  "displayName": "Selects a subset of fields from a DataFrame",
		  "description": "Creates a new DataFrame which is a subset of the provided DataFrame",
		  "type": "Pipeline",
		  "nextStepId": "GROUPDATASTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!ROOT.pipelineParameters.LOAD_DATA_PIPELINE.LOADFILESTEP.primaryReturn"
			},
			{
			  "type": "list",
			  "name": "fieldNames",
			  "required": true,
			  "value": [
				"product_id",
				"product_name",
				"cost"
			  ]
			}
		  ],
		  "engineMeta": {
			"spark": "MappingSteps.selectFields"
		  }
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
			  "value": "product_id"
			}
		  ],
		  "engineMeta": {
			"spark": "GroupingSteps.groupByField"
		  }
		}
	  ]
	}
  ],
  "parents": [
	"ROOT"
  ]
}
```

### Extract Customer Data Pipeline (CUST)
This pipeline will take the *DataFrame* loaded in the first execution pipeline and use it as a parameter for the first 
step in the pipeline. The *MAPFIELDSSTEP* relies on the execution id being **ROOT**. This pipeline needs to be part of 
an execution that is dependent on the *ROOT* execution created previously.

```json
{
  "id": "CUST",
  "pipelines": [
	{
	  "id": "EXTRACT_CUSTOMER_DATA_PIPELINE",
	  "name": "Extract Customer Data Pipeline",
	  "steps": [
		{
		  "id": "MAPFIELDSSTEP",
		  "stepId": "772912d6-ee6a-5228-ae7a-0127eb2dce37",
		  "displayName": "Selects a subset of fields from a DataFrame",
		  "description": "Creates a new DataFrame which is a subset of the provided DataFrame",
		  "type": "Pipeline",
		  "nextStepId": "GROUPDATASTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!ROOT.pipelineParameters.LOAD_DATA_PIPELINE.LOADFILESTEP.primaryReturn"
			},
			{
			  "type": "list",
			  "name": "fieldNames",
			  "required": true,
			  "value": [
				"customer_id",
				"first_name",
				"last_name",
				"email",
				"gender",
				"ein",
				"postal_code"
			  ]
			}
		  ],
		  "engineMeta": {
			"spark": "MappingSteps.selectFields"
		  }
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
			  "value": "customer_id"
			}
		  ],
		  "engineMeta": {
			"spark": "GroupingSteps.groupByField"
		  }
		}
	  ]
	}
  ],
  "parents": [
	"ROOT"
  ]
}
```
### Extract Credit Card Data Pipeline (CC)
This pipeline will take the *DataFrame* loaded in the first execution pipeline and use it as a parameter for the first 
step in the pipeline. The *MAPFIELDSSTEP* relies on the execution id being **ROOT**. This pipeline needs to be part of 
an execution that is dependent on the *ROOT* execution created previously.

```json
{
  "id": "CC",
  "pipelines": [
	{
	  "id": "EXTRACT_CREDIT_CARD_DATA_PIPELINE",
	  "name": "Extract Credit Card Data Pipeline",
	  "steps": [
		{
		  "id": "MAPFIELDSSTEP",
		  "stepId": "772912d6-ee6a-5228-ae7a-0127eb2dce37",
		  "displayName": "Selects a subset of fields from a DataFrame",
		  "description": "Creates a new DataFrame which is a subset of the provided DataFrame",
		  "type": "Pipeline",
		  "nextStepId": "GROUPDATASTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!ROOT.pipelineParameters.LOAD_DATA_PIPELINE.LOADFILESTEP.primaryReturn"
			},
			{
			  "type": "list",
			  "name": "fieldNames",
			  "required": true,
			  "value": [
				"customer_id",
				"cc_num",
				"cc_type"
			  ]
			}
		  ],
		  "engineMeta": {
			"spark": "MappingSteps.selectFields"
		  }
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
			  "value": "customer_id"
			}
		  ],
		  "engineMeta": {
			"spark": "GroupingSteps.groupByField"
		  }
		}
	  ]
	}
  ],
  "parents": [
	"ROOT"
  ]
}
```
### Extract Order Data Pipeline (ORD)
This pipeline will take the *DataFrame* loaded in the first execution pipeline and use it as a parameter for the first 
step in the pipeline. The *MAPFIELDSSTEP* relies on the execution id being **ROOT**. This pipeline needs to be part of 
an execution that is dependent on the *ROOT* execution created previously.

```json
{
  "id": "ORD",
  "pipelines": [
	{
	  "id": "EXTRACT_ORDER_DATA_PIPELINE",
	  "name": "Extract Order Data Pipeline",
	  "steps": [
		{
		  "id": "MAPFIELDSSTEP",
		  "stepId": "772912d6-ee6a-5228-ae7a-0127eb2dce37",
		  "displayName": "Selects a subset of fields from a DataFrame",
		  "description": "Creates a new DataFrame which is a subset of the provided DataFrame",
		  "type": "Pipeline",
		  "nextStepId": "GROUPDATASTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!ROOT.pipelineParameters.LOAD_DATA_PIPELINE.LOADFILESTEP.primaryReturn"
			},
			{
			  "type": "list",
			  "name": "fieldNames",
			  "required": true,
			  "value": [
				"order_num",
				"product_id",
				"units",
				"customer_id"
			  ]
			}
		  ],
		  "engineMeta": {
			"spark": "MappingSteps.selectFields"
		  }
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
			  "value": "order_num"
			}
		  ],
		  "engineMeta": {
			"spark": "GroupingSteps.groupByField"
		  }
		}
	  ]
	}
  ],
  "parents": [
	"ROOT"
  ]
}
```
## Final Execution
Now that the data has been loaded and processed into different forms, a final pipeline will be responsible for writing 
the data to a Mongo data store. This pipeline is only here to show a multi-parent dependency relationship. It would probably 
be more optimal to have each of the other pipelines write as a last step.

A new library (mongo connector) and a new step will be required.

First thing is to include the new library in the pom.xml file. Add this entry to the dependencies section:

```xml
<dependency>
	<groupId>org.mongodb.spark</groupId>
	<artifactId>mongo-spark-connector_${scala.compat.version}</artifactId>
	<version>2.3.1</version>
</dependency>
```
* Open the object in the *com.acxiom.pipeline.steps* package named [**InputOutputSteps**](src/main/scala/com/acxiom/pipeline/steps/InputOutputSteps.scala)
* Create a function named *writeDataFrameToMongo* and declare three parameters:
	* dataFrame: DataFrame
	* uri: String
	* collectionName: String
* Give the function a return type of Unit
* Insert the following code as the function:

```scala
def writeDatFrameToMongo(dataFrame: DataFrame, uri: String, collectionName: String): Unit =
			MongoSpark.save(dataFrame, WriteConfig(Map("collection" -> collectionName, "uri" -> uri)))
```
### Final Execution JSON
```json
{
  "id": "SAVE",
  "pipelines": [
	{
	  "id": "WRITE_DATA_PIPELINE",
	  "name": "Write Data Pipeline",
	  "steps": [
		{
		  "id": "PRODWRITESTEP",
		  "stepId": "6b9db56d-bed7-5838-9ed4-7b5e216617c4",
		  "displayName": "Writes a DataFrame to a Mongo database",
		  "description": "This step will write the contents of a DataFrame to the Mongo database and collection specified",
		  "type": "Pipeline",
		  "nextStepId": "CUSTWRITESTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!PROD.pipelineParameters.EXTRACT_PRODUCT_DATA_PIPELINE.GROUPDATASTEP.primaryReturn"
			},
			{
			  "type": "string",
			  "name": "uri",
			  "required": true,
			  "value": "!mongoURI"
			},
			{
			  "type": "string",
			  "name": "collectionName",
			  "required": true,
			  "value": "products"
			}
		  ],
		  "engineMeta": {
			"spark": "InputOutputSteps.writeDataFrameToMongo"
		  }
		},
		{
		  "id": "CUSTWRITESTEP",
		  "stepId": "6b9db56d-bed7-5838-9ed4-7b5e216617c4",
		  "displayName": "Writes a DataFrame to a Mongo database",
		  "description": "This step will write the contents of a DataFrame to the Mongo database and collection specified",
		  "type": "Pipeline",
		  "nextStepId": "CCWRITESTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!CUST.pipelineParameters.EXTRACT_CUSTOMER_DATA_PIPELINE.GROUPDATASTEP.primaryReturn"
			},
			{
			  "type": "string",
			  "name": "uri",
			  "required": true,
			  "value": "!mongoURI"
			},
			{
			  "type": "string",
			  "name": "collectionName",
			  "required": true,
			  "value": "customers"
			}
		  ],
		  "engineMeta": {
			"spark": "InputOutputSteps.writeDataFrameToMongo"
		  }
		},
		{
		  "id": "CCWRITESTEP",
		  "stepId": "6b9db56d-bed7-5838-9ed4-7b5e216617c4",
		  "displayName": "Writes a DataFrame to a Mongo database",
		  "description": "This step will write the contents of a DataFrame to the Mongo database and collection specified",
		  "type": "Pipeline",
		  "nextStepId": "ORDWRITESTEP",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!CC.pipelineParameters.EXTRACT_CREDIT_CARD_DATA_PIPELINE.GROUPDATASTEP.primaryReturn"
			},
			{
			  "type": "string",
			  "name": "uri",
			  "required": true,
			  "value": "!mongoURI"
			},
			{
			  "type": "string",
			  "name": "collectionName",
			  "required": true,
			  "value": "creditCards"
			}
		  ],
		  "engineMeta": {
			"spark": "InputOutputSteps.writeDataFrameToMongo"
		  }
		},
		{
		  "id": "ORDWRITESTEP",
		  "stepId": "6b9db56d-bed7-5838-9ed4-7b5e216617c4",
		  "displayName": "Writes a DataFrame to a Mongo database",
		  "description": "This step will write the contents of a DataFrame to the Mongo database and collection specified",
		  "type": "Pipeline",
		  "params": [
			{
			  "type": "string",
			  "name": "dataFrame",
			  "required": true,
			  "value": "!ORD.pipelineParameters.EXTRACT_ORDER_DATA_PIPELINE.GROUPDATASTEP.primaryReturn"
			},
			{
			  "type": "string",
			  "name": "uri",
			  "required": true,
			  "value": "!mongoURI"
			},
			{
			  "type": "string",
			  "name": "collectionName",
			  "required": true,
			  "value": "orders"
			}
		  ],
		  "engineMeta": {
			"spark": "InputOutputSteps.writeDataFrameToMongo"
		  }
		}
	  ]
	}
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
Since the configuration is completely configuration based, there is no need to create a new *DriverSetup*. The 
*ApplicationDriverSetup* has been provided to configure the execution plan. 

## Running
The code will need to be packaged as an 'uber-jar' (the example project does this automatically when package is called) 
that contains all of the dependencies. Once this is done, place the jar in a location that can be read by Spark.

Submit a job:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars <jar_path>/spark-pipeline-engine_2.11-<VERSION>.jar,<jar_path>/streaming-pipeline-drivers_2.11-<VERSION>.jar \
<jar_path>/pipeline-drivers-examples_2.11-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.ApplicationDataDriverSetup \
--applicationConfigPath <location of application-example.json> \
--input_url <location of input file> \
--input_format <csv, parquet, etc...> \
--pipelinesJson <path to the execution-pipelines.json file> \
--mongoURI <URI to connect to the Mongo DB> \
--logLevel DEBUG
```
