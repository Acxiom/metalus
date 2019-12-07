# Simple ETL Example
This example was taken from the [Spark Examples](http://spark.apache.org/examples.html) page and adapted to use the 
default pipeline driver. Below is a walk through of creating each of the required components required to read in a file,
perform counts against the data and write the counts to disk.

## InputOutputSteps
Two steps are required to read and write files. This could be a single operation, but in order to be more reusable input
and output will be broken apart.

* Create a new object in the *com.acxiom.pipeline.steps* package named [**InputOutputSteps**](src/main/scala/com/acxiom/pipeline/steps/InputOutputSteps.scala)
* Create a function name *loadFile* and declare four parameters:
	* url: String
	* format: String
	* separator: Option[String]
	* pipelineContext: PipelineContext
* Give the function a return type of DataFrame
* Insert the following code into the body of the function (**Note**: This code is slightly different in the example project):

```scala
val dfr = if (separator.isDefined) {
	pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head.toString)
} else {
	pipelineContext.sparkSession.get.read.format(format)
}

dfr.load(url)
```
	
* Create a function name *writeJSONFile* and declare two parameters:
	* dataFrame: DataFrame
	* url: String
* Give the function a return type of Unit
* Insert the following code into the body of the function:

```scala
dataFrame.write.format("json").save(url)
```

## GroupingSteps
There needs to be a step function created to do the counts.

* Create a new object in the *com.acxiom.pipeline.steps* package named [**GroupingSteps**](src/main/scala/com/acxiom/pipeline/steps/GroupingSteps.scala)
* Create a function name *countsByField* and declare two parameters:
	* dataFrame: DataFrame
	* fieldName: String
* Give the function a return type of DataFrame
* Insert the following code into the body of the function:

```scala
dataFrame.groupBy(fieldName).count()
```

## DriverSetup
The *DriverSetup* trait is the starting point of the application. The implementation is responsible for preparing the
PipelineContext as well as supplying the pipelines that will be executed.

* Create a new case class in *com.acxiom.pipeline* named [**SimpleDataDriverSetup**](src/main/scala/com/acxiom/pipeline/SimpleDataDriverSetup.scala).
* Extend **DriverSetup**
* Provide the following constructor:

```scala
(parameters: Map[String, Any])
```
* Initialize the SparkConf:

```scala
private val sparkConf = new SparkConf().set("spark.hadoop.io.compression.codecs",
			"org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
				"org.apache.hadoop.io.compress.GzipCodec,org.apache." +
				"hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")
```
* Initialize the SparkSession:

```scala
private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
```
* Initialize the PipelineContext:

```scala
private val ctx = PipelineContext(Some(sparkConf), Some(sparkSession), Some(parameters),
			PipelineSecurityManager(),
			PipelineParameters(List(PipelineParameter("SIMPLE_DATA_PIPELINE", Map[String, Any]()))),
			Some(if (parameters.contains("stepPackages")) {
				parameters("stepPackages").asInstanceOf[String]
					.split(",").toList
			} else {
				List("com.acxiom.pipeline.steps")
			}),
			PipelineStepMapper(),
			Some(DefaultPipelineListener()),
			Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
```
* Map the three step functions to PipelineStep objects:

```scala
private val LOAD_FILE = PipelineStep(Some("LOADFILESTEP"),
	Some("Load File as Data Frame"),
	Some("This step will load a file from the provided URL"), Some("Pipeline"),
	Some(List(Parameter(Some("text"), Some("url"), Some(true), None, Some("!input_url")),
		Parameter(Some("text"), Some("format"), Some(true), None, Some("!input_format")),
		Parameter(Some("text"), Some("separator"), Some(false), None, Some("!input_separator")))),
	Some(EngineMeta(Some("InputOutputSteps.loadFile"))),
	Some("PROCESSDFSTEP"))

private val PROCESS_DF = PipelineStep(Some("PROCESSDFSTEP"), Some("Counts By Field"),
	Some("Returns counts by the provided field name. The result is a data frame."), Some("Pipeline"),
	Some(List(Parameter(Some("text"), Some("fieldName"), Some(true), None, Some("!grouping_field")),
		Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("@LOADFILESTEP")))),
	Some(EngineMeta(Some("GroupingSteps.countsByField"))),
	Some("WRITEFILESTEP"))

private val WRITE_FILE = PipelineStep(Some("WRITEFILESTEP"), Some("Write Data Frame to a json file"),
	Some("This step will write a DataFrame from the provided URL"), Some("Pipeline"),
	Some(List(Parameter(Some("text"), Some("url"), Some(true), None, Some("!output_url")),
		Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("@PROCESSDFSTEP")))),
	Some(EngineMeta(Some("InputOutputSteps.writeJSONFile"))))
```

* **Note**: Special mapping instructions have been used to make this pipeline definition reusable:
	* **!input_url** - This will pull the value from the *input_url* global parameter. This should be provided as an 
application parameter.
	* **!input_format** - Like the *input_url* this will pull from the globals object.
	* **!grouping_field** - Like the *input_url* this will pull from the globals object.
	* **!output_url** - Like the *input_url* this will pull from the globals object.
	* **@LOADFILESTEP** - This will pull the primary return value of the *LOADFILESTEP* step.
	* **@PROCESSDFSTEP** - This will pull the primary return value of the *PROCESSDFSTEP* step.

* Override the *pipelines* function to return an empty List:

```scala
override def pipelines: List[Pipeline] = List()
```
* Override the *initialPipelineId* function to return an empty string.
* Override the *pipelineContext* function:

```scala
override def pipelineContext: PipelineContext = ctx
``` 
	
* Override the *executionPlan* function to return an list containing a single execution:

```scala
override def executionPlan: Option[List[PipelineExecution]] = Some(List(PipelineExecution("0",
	List(Pipeline(Some("SIMPLE_DATA_PIPELINE"), Some("Simple Data Example"),
		Some(List(LOAD_FILE, PROCESS_DF, WRITE_FILE)))), None, ctx, None)))
``` 

## Running
The code may be run using the provided [application jar](../../metalus-application/readme.md) for the main jar and the 
metalus-common and metalus-examples jars provided to the *--jars* parameter.

### Run the spark-submit command for Spark 2.3:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.3-<VERSION>.jar,metalus-examples_2.11-spark_2.3-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.3-<VERSION>.jar \
--driverSetupClass  \
--input_url <location of input file> \
--input_format <csv, parquet, etc...> \
--input_separator , \
--grouping_field <field name to group by> \
--output_url <location to write the JSON file \
--logLevel DEBUG
```

### Run the spark-submit command for Spark 2.4:

```bash
spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
--master spark://localhost:7077 \
--deploy-mode client \
--jars metalus-common_2.11-spark_2.4-<VERSION>.jar,metalus-examples_2.11-spark_2.4-<VERSION>.jar  \
<jar_path>/metalus-application_2.11-spark_2.4-<VERSION>.jar \
--driverSetupClass com.acxiom.pipeline.ExecutionPlanDataDriverSetup \
--input_url <location of input file> \
--input_format <csv, parquet, etc...> \
--input_separator , \
--grouping_field <field name to group by> \
--output_url <location to write the JSON file \
--logLevel DEBUG
```

