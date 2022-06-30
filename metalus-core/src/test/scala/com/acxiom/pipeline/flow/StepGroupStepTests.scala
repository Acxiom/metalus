package com.acxiom.pipeline.flow

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.File

class StepGroupStepTests extends FunSpec with BeforeAndAfterAll with Suite {
  override def beforeAll(): Unit = {
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

  override def afterAll(): Unit = {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  describe("Verify validations") {
    it("Should fail validation with missing parameters") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("bad-id"), Some("invalid-pipeline"),
        Some(List(PipelineStep(Some("step-id"),
          None, None, Some("step-group"), Some(List())))))),
        None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }
  }

  describe("Should execute step groups") {
    val subPipelineStepOne = PipelineStep(Some("SUB_PIPELINE_STEP_ONE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"),
        value = Some("!globalOne || !realGlobalOne || ONE")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))), nextStepId = Some("SUB_PIPELINE_STEP_TWO"))
    val subPipelineStepTwo = PipelineStep(Some("SUB_PIPELINE_STEP_TWO"), Some("Sub Pipeline Step Two"), None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalTwo || TWO")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepGlobalsUpdateFunction"))), nextStepId = Some("SUB_PIPELINE_STEP_THREE"))
    val subPipelineStepThree = PipelineStep(Some("SUB_PIPELINE_STEP_THREE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalThree || THREE")),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
    val subPipeline = DefaultPipeline(Some("subPipelineId"), Some("Sub Pipeline"), Some(List(
      subPipelineStepOne, subPipelineStepTwo, subPipelineStepThree)), category = Some("step-group"))

    implicit val formats: Formats = DefaultFormats
    val json = Serialization.write(subPipeline)

    val pipelineStepOne = PipelineStep(Some("PIPELINE_STEP_ONE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalOne || ONE")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))), nextStepId = Some("PIPELINE_STEP_TWO"))
    val pipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
      Some(List(Parameter(Some("text"), Some("pipeline"),
        className = Some("com.acxiom.pipeline.DefaultPipeline"),
        value = parse(json).extractOpt[Map[String, Any]]),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))
    val pipelineStepThree = PipelineStep(Some("PIPELINE_STEP_THREE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalThree || THREE")),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
    val pipeline = DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
      pipelineStepOne, pipelineStepTwo, pipelineStepThree)))

    it("Should execute step with no pipelineMappings") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext)
    }

    it("Should execute step with pipelineMappings pulled from globals") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings and globals") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings pulled from PipelineManager") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipelineId"),
          value = Some("subPipelineId")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .copy(pipelineManager = PipelineManager(List(subPipeline)))
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings pulled from PipelineManager using special character") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("&subPipelineId")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .copy(pipelineManager = PipelineManager(List(subPipeline)))
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with result parameter") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))),
        Parameter(Some("result"), Some("output"), None, None, Some("@SUB_PIPELINE_STEP_TWO")))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val parameters = ctx.parameters.getParametersByPipelineId("pipelineId").get
      val response = parameters.parameters("PIPELINE_STEP_TWO").asInstanceOf[PipelineStepResponse]
      assert(response.primaryReturn.contains("gtwo"))
      assert(response.namedReturns.isDefined)
    }

    it("Should execute step with result parameter using inline script") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))),
          Parameter(Some("result"), Some("output"), None, None, Some(" (value:!globalThree:String) value")))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = SparkTestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val parameters = ctx.parameters.getParametersByPipelineId("pipelineId").get
      val response = parameters.parameters("PIPELINE_STEP_TWO").asInstanceOf[PipelineStepResponse]
      assert(response.primaryReturn.contains("3"))
      assert(response.namedReturns.isDefined)
    }

    it("Should execute step with stepGroupResult") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStep(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        engineMeta = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val stepGroup = DefaultPipeline(Some("subPipelineId"), Some("Sub Pipeline"), Some(List(
        subPipelineStepOne,
        subPipelineStepTwo.copy(params = Some(List(
          Parameter(Some("text"), Some("string"), value = Some("!globalTwo || TWO")),
          Parameter(Some("boolean"), Some("boolean"), value = Some(false)),
          Parameter(Some("text"), Some("global"), value = Some("This global has been updated"))
        ))),
        subPipelineStepThree)), category = Some("step-group"),
        stepGroupResult = Some("@SUB_PIPELINE_STEP_TWO"))
      val context = SparkTestHelper.generatePipelineContext().setGlobal("subPipeline", stepGroup)
      val executionResult = PipelineExecutor.executePipelines(List(DefaultPipeline(Some("pipelineId"),
        Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree)))), None, context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val parameters = ctx.parameters.getParametersByPipelineId("pipelineId").get
      val response = parameters.parameters("PIPELINE_STEP_TWO").asInstanceOf[PipelineStepResponse]
      assert(response.primaryReturn.contains("gtwo"))
      assert(response.namedReturns.isDefined)
      assert(ctx.getGlobalString("updatedGlobal").getOrElse("") == "")
      assert(ctx.getGlobalString("mockGlobal").getOrElse("") == "This global has been updated")
      assert(ctx.getGlobalString("mockGlobalLink").getOrElse("") == "!some global link")
    }

    it ("Should detect result script") {
      val context = SparkTestHelper.generatePipelineContext()
        .setGlobal("globalResult", "globalResult")
      val mapper = PipelineStepMapper()
      assert(mapper.mapParameter(Parameter(Some("result"), value = Some(" (value:!globalResult:String) value")), context)
        == "globalResult")
      assert(mapper.mapParameter(Parameter(Some("result"), value = Some("test")), context) == "test")
    }

    def validateResults(ctx: PipelineContext,
                        subStepOneValue: String = "ONE",
                        subStepTwoValue: String = "TWO",
                        subStepThreeValue: String = "THREE"): Unit = {
      assert(ctx.parameters.getParametersByPipelineId("pipelineId").isDefined)
      val parameters = ctx.parameters.getParametersByPipelineId("pipelineId").get
      assert(parameters.parameters.size == 3)
      assert(parameters.parameters.contains("PIPELINE_STEP_ONE"))
      assert(parameters.parameters("PIPELINE_STEP_ONE").asInstanceOf[PipelineStepResponse].primaryReturn.contains("ONE"))
      assert(parameters.parameters.contains("PIPELINE_STEP_THREE"))
      assert(parameters.parameters("PIPELINE_STEP_THREE").asInstanceOf[PipelineStepResponse].primaryReturn.contains("THREE"))
      assert(parameters.parameters.contains("PIPELINE_STEP_TWO"))
      assert(parameters.parameters("PIPELINE_STEP_TWO").isInstanceOf[PipelineStepResponse])
      val response = parameters.parameters("PIPELINE_STEP_TWO").asInstanceOf[PipelineStepResponse]
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get.isInstanceOf[Map[String, PipelineStepResponse]])
      assert(response.namedReturns.isEmpty)
      val subResponse = response.primaryReturn.get.asInstanceOf[Map[String, PipelineStepResponse]]
      assert(subResponse.size == 3)
      assert(subResponse.contains("SUB_PIPELINE_STEP_ONE"))
      assert(subResponse("SUB_PIPELINE_STEP_ONE").primaryReturn.contains(subStepOneValue))
      assert(subResponse.contains("SUB_PIPELINE_STEP_TWO"))
      assert(subResponse("SUB_PIPELINE_STEP_TWO").primaryReturn.contains(subStepTwoValue))
      assert(subResponse.contains("SUB_PIPELINE_STEP_THREE"))
      assert(subResponse("SUB_PIPELINE_STEP_THREE").primaryReturn.contains(subStepThreeValue))

      // Verify audits are handled properly
      assert(ctx.rootAudit.children.isDefined)
      assert(ctx.rootAudit.children.get.length == 1)
      val pipelineAudit = ctx.rootAudit.children.get.head
      assert(pipelineAudit.children.isDefined)
      assert(pipelineAudit.children.get.length == 3)
      val stepGroupAudit = pipelineAudit.children.get(1)
      assert(stepGroupAudit.children.isDefined)
      assert(stepGroupAudit.children.get.length == 1)
      val subPipelineAudit = stepGroupAudit.children.get.head
      assert(subPipelineAudit.id == "subPipelineId")
      assert(subPipelineAudit.children.isDefined)
      assert(subPipelineAudit.children.get.length == 3)
      assert(subPipelineAudit.children.get.head.id == "SUB_PIPELINE_STEP_ONE")
      assert(subPipelineAudit.children.get(1).id == "SUB_PIPELINE_STEP_TWO")
      assert(subPipelineAudit.children.get(2).id == "SUB_PIPELINE_STEP_THREE")
    }
  }
}
