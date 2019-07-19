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
  private val generateDataStep = PipelineStep(Some("GENERATE_DATA"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("integer"), Some("listSize"), value = Some(3)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStringListStepFunction"))),
    nextStepId = Some("FORK_DATA"))
  private val simpleForkSerialStep = PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
    Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
      Parameter(Some("text"), Some("forkMethod"), value = Some("serial")))),
    nextStepId = Some("PROCESS_VALUE"))
  private val processValueStep = PipelineStep(Some("PROCESS_VALUE"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@FORK_DATA")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val simpleBranchStep = PipelineStep(Some("BRANCH_VALUE"), None, None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@PROCESS_VALUE")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)),
      Parameter(Some("result"), Some("0"), value = Some("JOIN")),
      Parameter(Some("result"), Some("1"), value = Some("JOIN")),
      Parameter(Some("result"), Some("2"), value = Some("JOIN")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val errorBranchStep = PipelineStep(Some("BRANCH_VALUE"), None, None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@PROCESS_VALUE")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)),
      Parameter(Some("result"), Some("0"), value = Some("JOIN")),
      Parameter(Some("result"), Some("1"), value = Some("EXCEPTION")),
      Parameter(Some("result"), Some("2"), value = Some("EXCEPTION")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val errorValueStep = PipelineStep(Some("EXCEPTION"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@FORK_DATA")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockExceptionStepFunction"))))

  describe("Fork Step Without Join") {
    it("Should process list and merge results using serial processing") {
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(List(generateDataStep, simpleForkSerialStep, processValueStep)))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }

    it("Should process list and merge results using parallel processing") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep)
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }
  }

  describe("Fork Step With Join") {
    it("Should process list and merge results using serial processing") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        simpleBranchStep, joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }

    it("Should process list and merge results using parallel processing") {
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(
        List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
          simpleBranchStep, joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      ))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }
  }

  describe("Verify validations") {
    it("Should fail if more than one fork is encountered in serial") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = Some("EXTRA_JOIN")), joinStep.copy(id = Some("EXTRA_JOIN")))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }

    it("Should fail if more than one fork is encountered in parallel") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = Some("EXTRA_JOIN")), joinStep.copy(id = Some("EXTRA_JOIN")))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }

    it("Should fail if forkMethod is not populated correctly"){
      val pipelineStepsWithoutForkMethod = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA"))))),
        processValueStep,
        joinStep)
      val pipelineWithoutForkMethod = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithoutForkMethod))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResultWithoutForkMethod = PipelineExecutor.executePipelines(List(pipelineWithoutForkMethod), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResultWithoutForkMethod.success)
      val pipelineStepsWithTypo = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
          Parameter(Some("text"), Some("forkMethod"), value = Some("seiral"))))),
        processValueStep,
        joinStep)
      val pipelineWithTypo = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithTypo))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResultWithTypo = PipelineExecutor.executePipelines(List(pipelineWithTypo), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResultWithTypo.success)
    }

    it("Should fail if forkByValues is not populated"){
      val pipelineStepsWithoutForkMethod = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkMethod"), value = Some("serial"))))),
        processValueStep,
        joinStep)
      val pipelineWithoutForkMethod = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithoutForkMethod))
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResultWithoutForkMethod = PipelineExecutor.executePipelines(List(pipelineWithoutForkMethod), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResultWithoutForkMethod.success)
    }
  }

  describe("Verify Exception Handling") {
    val message =
      """One or more errors has occurred while processing fork step:
        | Execution 1: exception thrown for string value (1)
        | Execution 2: exception thrown for string value (2)
        |""".stripMargin
    it("Should process list and handle exception using serial processing") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        errorBranchStep, errorValueStep.copy(nextStepId = Some("JOIN")), joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      val results = new ListenerValidations
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(
                "One of the executions should have failed!",
                valid = e.getMessage == message)
          }
        }
      }
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      results.validate()
    }

    it("Should process list and handle exception using parallel processing") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        errorBranchStep, errorValueStep.copy(nextStepId = Some("JOIN")), joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      val results = new ListenerValidations
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(
                "One of the executions should have failed!",
                valid = e.getMessage == message)
          }
        }
      }
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      results.validate()
    }
  }

  private def verifySimpleForkSteps(pipeline: Pipeline, executionResult: PipelineExecutionResult, extraStep: Boolean = false) = {
    assert(executionResult.success)
    val ctx = executionResult.pipelineContext
    assert(ctx.getGlobalString("forkId").isEmpty)
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
    assert(ctx.rootAudit.children.isDefined)
    assert(ctx.rootAudit.children.get.length == 1)
    val pipelineAudit = ctx.rootAudit.children.get.head
    assert(pipelineAudit.children.isDefined)

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
      assert(pipelineAudit.children.get.length == 12)
    } else {
      assert(pipelineAudit.children.get.length == 5)
    }
  }
}
