package com.acxiom.pipeline

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

class ForkJoinStepTests extends FunSpec with BeforeAndAfterAll with Suite {
  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      ",org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

    SparkTestHelper.sparkSession = SparkSession.builder().config(SparkTestHelper.sparkConf).getOrCreate()

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  override def afterAll() {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  private val simpleForkParallelStep = PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
    Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
      Parameter(Some("text"), Some("forkMethod"), value = Some("parallel")))),
    nextStepId = Some("PROCESS_VALUE"))
  private val simpleMockStep = PipelineStep(Some("PROCESS_RAW_VALUE"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("RAW_DATA")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val joinStep = PipelineStep(Some("JOIN"), None, None, Some("join"), None, None)
  private val simplePipelineSteps = List(
    PipelineStep(Some("GENERATE_DATA"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("integer"), Some("listSize"), value = Some(3)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStringListStepFunction"))),
      nextStepId = Some("FORK_DATA")),
    PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
      Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
        Parameter(Some("text"), Some("forkMethod"), value = Some("serial")))),
      nextStepId = Some("PROCESS_VALUE")),
    PipelineStep(Some("PROCESS_VALUE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("@FORK_DATA")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  )

  describe("Fork Step Without Join") {
    it("Should process list and merge results using serial processing") {
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(simplePipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }

    it("Should process list and merge results using parallel processing") {
      val pipelineSteps = List(simplePipelineSteps.head, simpleForkParallelStep, simplePipelineSteps(2))
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }
  }

  describe("Fork Step With Join") {
    it("Should process list and merge results using serial processing") {
      val pipelineSteps = List(simplePipelineSteps.head, simplePipelineSteps(1), simplePipelineSteps(2).copy(nextStepId = Some("JOIN")),
        joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }

    it("Should process list and merge results using parallel processing") {
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(
        List(simplePipelineSteps.head, simpleForkParallelStep, simplePipelineSteps(2).copy(nextStepId = Some("JOIN")),
          joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      ))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }
  }

  describe("Verify validations") {
    it("Should fail if more than one fork is encountered in serial") {
      val pipelineSteps = List(simplePipelineSteps.head, simplePipelineSteps(1), simplePipelineSteps(2).copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = Some("EXTRA_JOIN")), joinStep.copy(id = Some("EXTRA_JOIN")))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }

    it("Should fail if more than one fork is encountered in parallel") {
      val pipelineSteps = List(simplePipelineSteps.head, simpleForkParallelStep, simplePipelineSteps(2).copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = Some("EXTRA_JOIN")), joinStep.copy(id = Some("EXTRA_JOIN")))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }
  }

  private def verifySimpleForkSteps(pipeline: Pipeline, executionResult: PipelineExecutionResult, extraStep: Boolean = false) = {
    assert(executionResult.success)
    val ctx = executionResult.pipelineContext
    val parameters = ctx.parameters.getParametersByPipelineId(pipeline.id.get).get
    assert(parameters.parameters.contains("GENERATE_DATA"))
    assert(parameters.parameters.contains("PROCESS_VALUE"))
    // Verify that the results were merged properly for each step
    val results = parameters.parameters("PROCESS_VALUE").asInstanceOf[PipelineStepResponse]
    assert(results.primaryReturn.isDefined)
    val primaryList = results.primaryReturn.get.asInstanceOf[List[Option[String]]]
    assert(primaryList.length == 3)
    assert(primaryList.head.getOrElse("wrong") == "0")
    assert(primaryList(1).getOrElse("wrong") == "1")
    assert(primaryList(2).getOrElse("wrong") == "2")
    // Verify the namedReturns
    assert(results.namedReturns.isDefined)
    val namedReturns = results.namedReturns.get
    assert(namedReturns.size == 2)
    assert(namedReturns.contains("boolean"))
    val booleanList = namedReturns("boolean").asInstanceOf[List[Option[Boolean]]]
    assert(booleanList.length == 3)
    assert(namedReturns.contains("string"))
    val stringList = namedReturns("string").asInstanceOf[List[Option[String]]]
    assert(stringList.length == 3)
    assert(stringList.head.get == "0")
    assert(stringList(1).get == "1")
    assert(stringList(2).get == "2")

    if (extraStep) {
      assert(parameters.parameters.contains("PROCESS_RAW_VALUE"))
      val raw =  parameters.parameters("PROCESS_RAW_VALUE").asInstanceOf[PipelineStepResponse]
      assert(raw.primaryReturn.isDefined)
      assert(raw.primaryReturn.get.asInstanceOf[String] == "RAW_DATA")
      assert(raw.namedReturns.isDefined)
      val rawNamedReturns = raw.namedReturns.get
      assert(rawNamedReturns.size == 2)
      assert(rawNamedReturns.contains("boolean"))
      assert(!rawNamedReturns("boolean").asInstanceOf[Boolean])
      assert(rawNamedReturns.contains("string"))
      assert(rawNamedReturns("string").asInstanceOf[String] == "RAW_DATA")
    }
  }
}
