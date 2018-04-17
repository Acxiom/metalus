package com.acxiom.pipeline

import java.io.File

import com.acxiom.pipeline.drivers.{DefaultPipelineDriver, DriverSetup}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

class SparkSuiteTests extends FunSpec with BeforeAndAfterAll with Suite {
  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.datalake").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      "org.apache.hadoop.io.compress.ZFramedCodec,org.apache.hadoop.io." +
        "compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
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

  describe("DefaultPipelineDriver") {
    it("Should run a basic pipeline") {
      val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value")
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Unit = {
          step.id.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              pipelineContext.parameters.getParametersByPipelineId("1").get.parameters("GLOBALVALUESTEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case _ =>
          }
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case pe: PipelineException =>
              assert(pe.pipelineId.getOrElse("") == "1")
              assert(pe.stepId.getOrElse("") == "PAUSESTEP")
          }
        }
      }
      // Execution should complete without exception
      new DefaultPipelineDriver().main(args.toArray)
    }
  }
}

object SparkTestHelper {
  val MASTER = "local[2]"
  val APPNAME = "file-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineListener: PipelineListener = _

  val GLOBAL_VALUE_STEP = PipelineStep(Some("GLOBALVALUESTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), Some("PAUSESTEP"))
  val PAUSE_STEP = PipelineStep(Some("PAUSESTEP"), Some("Pause Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("@GLOBALVALUESTEP")))),
    Some(EngineMeta(Some("MockPipelineSteps.pauseStep"))))
  val BASIC_PIPELINE = List(Pipeline(Some("1"), Some("Basic Pipeline"), Some(List(GLOBAL_VALUE_STEP, PAUSE_STEP))))
}

case class SparkTestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  override def pipelines: List[Pipeline] = parameters("pipeline") match {
    case "basic" => SparkTestHelper.BASIC_PIPELINE
  }

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = {
    PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession), Some(parameters),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("1", Map[String, Any]()))),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      } else {
        List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")
      }),
      PipelineStepMapper(),
      Some(SparkTestHelper.pipelineListener),
      Some(SparkTestHelper.sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }
}

object MockPipelineSteps {
  def globalVariableStep(string: String, pipelineContext: PipelineContext): String = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
    pipelineContext.addStepMessage(PipelineStepMessage(string, stepId, pipelineId, PipelineStepMessageType.warn))
    string
  }

  def pauseStep(string: String, pipelineContext: PipelineContext): String = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
    pipelineContext.addStepMessage(PipelineStepMessage(string, stepId, pipelineId, PipelineStepMessageType.pause))
    string
  }
}
