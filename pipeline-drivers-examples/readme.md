# Spark Pipeline Driver Examples
This project contains examples demonstrating how to get started.

## Simple Data Operations example
This example was taken from the [Spark Examples](http://spark.apache.org/examples.html) page and adapted.

### Example Code
This sample was taken from the [Spark Examples](http://spark.apache.org/examples.html) page and converted to use the
Pipeline Engine. All of the code exists in this project as a way to quick start, however below is a walk through of 
creating each of the required components.

#### InputOutputSteps
Two steps are required to read and write files. This could be a single operation, but in order to be more reusable input
and output will be broken apart.

* Created a new object in the *com.acxiom.pipeline.steps* package named [**InputOutputSteps**](src/main/scala/com/acxiom/pipeline/steps/InputOutputSteps.scala)
* Created a function name *loadFile* and declare four parameters:
	* url: String
	* format: String
	* separator: Option[String]
	* pipelineContext: PipelineContext
* Gave the function a return type of DataFrame
* Inserted the following code into the body of the function (**Note**: This code is slightly different in the example project):

	```scala
	val dfr = if (separator.isDefined) {
	  pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head)
	} else {
	  pipelineContext.sparkSession.get.read.format(format)
	}

	dfr.load(url)
	```
	
* Created a function name *writeJSONFile* and declare two parameters:
	* dataFrame: DataFrame
	* url: String
* Give the function a return type of Unit
* Insert the following code into the body of the function:

	```scala
	dataFrame.write.format("json").save(url)
	```

#### GroupingSteps
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

#### DriverSetup
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

#### PipelineStep
While creating the *DriverSetup*, three **PipelineStep**s were also created. Four global values *(beginning with !)* were
used when creating the steps. Additionally, the last two steps used the step reference *(@)* character to pull the return
value from a previous step and used to populate a function parameter. 

### Running
The code will need to be packaged as an uber jar (the example project does this automatically when package is called) that
contains all of the dependencies. Once this is done, place the jar in a location that can be read by Spark.

Submit a job:

	```
	spark-submit --class com.acxiom.pipeline.drivers.DefaultPipelineDriver \
	--master spark://localhost:7077 \
	--deploy-mode client \
	--jars <jar_path/spark-pipeline-engine_2.11-<VERSION>.jar,<jar_path/streaming-pipeline-drivers_2.11-<VERSION>.jar
	<jar_path>/pipeline-drivers-examples_2.11-<VERSION>.jar \
	--driverSetupClass com.acxiom.pipeline.SimpleDataDriverSetup \
	--input_url <location of input file> \
	--input_format <csv, parquet, etc...> \
	--grouping_field <field name to group by> \
	--output_url <location to write the JSON file
	```

## Simple Streaming Example
A [**simple Kinesis streaming example**](docs/kinesis-streaming-example.md) is also available.