package com.acxiom.pipeline.steps

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import java.nio.file.{Files, Path}

class FlowUtilsStepsTests extends FunSpec with BeforeAndAfterAll {
  val MASTER = "local[2]"
  val APPNAME = "flow-utils-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")

  val STRING_STEP: PipelineStep = PipelineStep(Some("STRINGSTEP"), Some("String Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("value"), Some(true), None, Some("lowercase")))),
    Some(EngineMeta(Some("StringSteps.toUpperCase"))), Some("RETRY"))

  val RETRY_STEP: PipelineStep = PipelineStep(Some("RETRY"), Some("Retry Step"), None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("counterName"), Some(true), None, Some("TEST_RETRY_COUNTER")),
      Parameter(Some("int"), Some("maxRetries"), Some(true), None, Some(Constants.FIVE)),
      Parameter(Some("result"), Some("retry"), Some(true), None, Some("STRINGSTEP")))),
    Some(EngineMeta(Some("FlowUtilsSteps.simpleRetry"))), None)

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("FlowUtilsSteps - Retry") {
    it("Should retry and trigger stop") {
      val pipeline = Pipeline(Some("testPipeline"), Some("retryPipeline"), Some(List(STRING_STEP, RETRY_STEP)))
      val initialPipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
        PipelineSecurityManager(),
        PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
        Some(List("com.acxiom.pipeline.steps")),
        PipelineStepMapper(),
        Some(DefaultPipelineListener()),
        Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
      val result = PipelineExecutor.executePipelines(List(pipeline), None, initialPipelineContext)
      val counter = result.pipelineContext.getGlobalAs[Int]("TEST_RETRY_COUNTER")
      assert(counter.isDefined)
      assert(counter.get == Constants.FIVE)
    }
  }
}
