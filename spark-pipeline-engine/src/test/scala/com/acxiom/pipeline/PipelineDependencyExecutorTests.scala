package com.acxiom.pipeline

import java.io.File
import java.util.Date

import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen, Suite}

import scala.collection.mutable

class PipelineDependencyExecutorTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen with Suite {

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

  private def generatePipelineContext(): PipelineContext = {
    val parameters = Map[String, Any]()
    PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession), Some(parameters),
      PipelineSecurityManager(),
      PipelineParameters(),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      }
      else {
        List("com.acxiom.pipeline.steps", "com.acxiom.pipeline")
      }),
      PipelineStepMapper(),
      Some(SparkTestHelper.pipelineListener),
      Some(SparkTestHelper.sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  describe("Execution Plan") {
    val pipelineJson =
      """
        |[
        | {
        |   "id": "Pipeline1",
        |   "name": "Pipeline 1",
        |   "steps": [
        |     {
        |       "id": "Pipeline1Step1",
        |       "displayName": "Pipeline1Step1",
        |       "type": "preload",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "value",
        |           "required": true,
        |           "value": "$value"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "ExecutionSteps.normalFunction"
        |       }
        |     }
        |   ]
        | }
        |]
      """.stripMargin
    val pipeline2Json =
      """
        |[
        | {
        |   "id": "Pipeline2",
        |   "name": "Pipeline 2",
        |   "steps": [
        |     {
        |       "id": "Pipeline2Step1",
        |       "displayName": "Pipeline2Step1",
        |       "type": "preload",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "value",
        |           "required": true,
        |           "value": "$value"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "ExecutionSteps.normalFunction"
        |       }
        |     }
        |   ]
        | }
        |]
      			""".stripMargin
    val pipeline3Json =
      """
        |[
        | {
        |   "id": "Pipeline3",
        |   "name": "Pipeline 3",
        |   "steps": [
        |     {
        |       "id": "Pipeline3Step1",
        |       "displayName": "Pipeline3Step1",
        |       "nextStepId": "Pipeline3Step2",
        |       "type": "preload",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "value",
        |           "required": true,
        |           "value": "$value"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "ExecutionSteps.normalFunction"
        |       }
        |     },
        |     {
        |       "id": "Pipeline3Step2",
        |       "displayName": "Pipeline3Step2",
        |       "type": "preload",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "value",
        |           "required": true,
        |           "value": "$secondValue"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "ExecutionSteps.normalFunction"
        |       }
        |     }
        |   ]
        | }
        |]
      			""".stripMargin
    it("Should execute a single list of pipelines") {
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          assert(getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Fred")
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ => fail("Unexpected exception registered")
          }
        }
      }
      val pipelines = DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Fred"))
      PipelineDependencyExecutor.executePlan(List(PipelineExecution("0", pipelines.get, None, generatePipelineContext(), None)))
    }

    it("Should execute a simple dependency graph of two pipeline chains") {
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              assert(getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Fred")
            case "Pipeline2" =>
              assert(getStringValue(pipelineContext, "Pipeline2", "Pipeline2Step1") == "Fred")
          }
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ => fail("Unexpected exception registered")
          }
        }
      }
      val pipelines1 = DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Fred"))
      val pipelines2 = DriverUtils.parsePipelineJson(pipeline2Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn"))
      PipelineDependencyExecutor.executePlan(List(
        PipelineExecution("0", pipelines1.get, None, generatePipelineContext(), None),
        PipelineExecution("1", pipelines2.get, None, generatePipelineContext(), Some(List("0")))))
    }

    it("Should execute a multi-tiered chain of dependencies") {
      val resultBuffer = mutable.ListBuffer[String]()
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              assert(getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              assert(getStringValue(pipelineContext, "PipelineChain1", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain1", "Pipeline3Step2") == "Chain1")
            case "PipelineChain2" =>
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              assert(getStringValue(pipelineContext, "Pipeline3", "Pipeline3Step1") == "Chain1")
              assert(getStringValue(pipelineContext, "Pipeline3", "Pipeline3Step2") == "Chain3")
            case "PipelineChain5" =>
              assert(getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              resultBuffer += s"Failed: ${exception.getCause.getMessage}"
              fail("Unexpected exception registered")
          }
        }
      }
      PipelineDependencyExecutor.executePlan(List(
        PipelineExecution("0", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain0")).get, None, generatePipelineContext(), None),
        PipelineExecution("1",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("\"Pipeline3\"", "\"PipelineChain1\"")
            .replace("$secondValue", "Chain1")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("2",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("ExecutionSteps.normalFunction", "ExecutionSteps.sleepFunction")
            .replace("\"Pipeline3\"", "\"PipelineChain2\"")
            .replace("$secondValue", "Chain2")).get, None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("3",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("\"Pipeline3\"", "\"PipelineChain3\"")
            .replace("$secondValue", "Chain3")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("4",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!1.pipelineParameters.PipelineChain1.Pipeline3Step2.primaryReturn")
            .replace("$secondValue", "!3.pipelineParameters.PipelineChain3.Pipeline3Step2.primaryReturn")).get,
          None, generatePipelineContext(), Some(List("1", "3"))),
        PipelineExecution("5", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain5")
          .replace("\"Pipeline1\"", "\"PipelineChain5\"")).get, None, generatePipelineContext(), None)
      ))

      val validResults = List("Pipeline1", "PipelineChain1", "PipelineChain2", "PipelineChain3", "PipelineChain5", "Pipeline3")
      resultBuffer.toList.foreach(value => assert(validResults.contains(value), s"Failed $value"))
      assert(resultBuffer.length == 6)
    }

    it("Should not execute child when one parent fails with an exception") {
      val resultBuffer = mutable.ListBuffer[String]()
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              assert(getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              resultBuffer += "Should not have called PipelineChain1"
              fail("Should not have called PipelineChain1")
            case "PipelineChain2" =>
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              resultBuffer += "Should not have called Pipeline3"
              fail("Should not have called Pipeline3")
            case "PipelineChain5" =>
              assert(getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              resultBuffer += exception.getCause.getMessage
              fail("Unexpected exception registered")
          }
        }
      }
      PipelineDependencyExecutor.executePlan(List(
        PipelineExecution("0", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain0")).get, None, generatePipelineContext(), None),
        PipelineExecution("1",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("ExecutionSteps.normalFunction", "ExecutionSteps.exceptionStep")
            .replace("\"Pipeline3\"", "\"PipelineChain1\"")
            .replace("$secondValue", "Chain1")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("2",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("ExecutionSteps.normalFunction", "ExecutionSteps.sleepFunction")
            .replace("\"Pipeline3\"", "\"PipelineChain2\"")
            .replace("$secondValue", "Chain2")).get, None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("3",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("\"Pipeline3\"", "\"PipelineChain3\"")
            .replace("$secondValue", "Chain3")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("4",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!1.pipelineParameters.PipelineChain1.Pipeline3Step2.primaryReturn")
            .replace("$secondValue", "!3.pipelineParameters.PipelineChain3.Pipeline3Step2.primaryReturn")).get,
          None, generatePipelineContext(), Some(List("1", "3"))),
        PipelineExecution("5", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain5")
          .replace("\"Pipeline1\"", "\"PipelineChain5\"")).get, None, generatePipelineContext(), None)
      ))
      // PipelineChain1 should throw an exception which h=should prevent Pipeline3 from executing
      val validResults = List("Pipeline1", "PipelineChain2", "PipelineChain3", "PipelineChain5")
      resultBuffer.toList.foreach(value => assert(validResults.contains(value), s"Failed $value"))
      assert(resultBuffer.length == 4)
    }

    it("Should not execute child when one parent pauses") {
      val resultBuffer = mutable.ListBuffer[String]()
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              assert(getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              resultBuffer += "Should not have called PipelineChain1"
              fail("Should not have called PipelineChain1")
            case "PipelineChain2" =>
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0")
              assert(getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              resultBuffer += "Should not have called Pipeline3"
              fail("Should not have called Pipeline3")
            case "PipelineChain5" =>
              assert(getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              resultBuffer += exception.getCause.getMessage
              fail("Unexpected exception registered")
          }
        }
      }
      PipelineDependencyExecutor.executePlan(List(
        PipelineExecution("0", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain0")).get, None, generatePipelineContext(), None),
        PipelineExecution("1",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("ExecutionSteps.normalFunction", "ExecutionSteps.pauseStep")
            .replace("\"Pipeline3\"", "\"PipelineChain1\"")
            .replace("$secondValue", "Chain1")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("2",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("ExecutionSteps.normalFunction", "ExecutionSteps.sleepFunction")
            .replace("\"Pipeline3\"", "\"PipelineChain2\"")
            .replace("$secondValue", "Chain2")).get, None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("3",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!0.pipelineParameters.Pipeline1.Pipeline1Step1.primaryReturn")
            .replace("\"Pipeline3\"", "\"PipelineChain3\"")
            .replace("$secondValue", "Chain3")).get,
          None, generatePipelineContext(), Some(List("0"))),
        PipelineExecution("4",
          DriverUtils.parsePipelineJson(pipeline3Json.replace("$value", "!1.pipelineParameters.PipelineChain1.Pipeline3Step2.primaryReturn")
            .replace("$secondValue", "!3.pipelineParameters.PipelineChain3.Pipeline3Step2.primaryReturn")).get,
          None, generatePipelineContext(), Some(List("1", "3"))),
        PipelineExecution("5", DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Chain5")
          .replace("\"Pipeline1\"", "\"PipelineChain5\"")).get, None, generatePipelineContext(), None)
      ))
      // PipelineChain1 should throw an exception which h=should prevent Pipeline3 from executing
      val validResults = List("Pipeline1", "PipelineChain2", "PipelineChain3", "PipelineChain5")
      resultBuffer.toList.foreach(value => assert(validResults.contains(value), s"Failed $value"))
      assert(resultBuffer.length == 4)
    }
  }

  private def getStringValue(pipelineContext: PipelineContext, pipelineId: String, stepId: String): String = {
    val response = pipelineContext.parameters.getParametersByPipelineId(pipelineId).get.parameters(stepId).asInstanceOf[PipelineStepResponse]
    response.primaryReturn.getOrElse("").asInstanceOf[String]
  }
}

object ExecutionSteps {
  private val ONE_SEC = 1000

  def sleepFunction(value: String): PipelineStepResponse = {
    Thread.sleep(ONE_SEC)
    PipelineStepResponse(Some(value), Some(Map[String, Any]("time" -> new Date())))
  }

  def normalFunction(value: String): PipelineStepResponse = {
    PipelineStepResponse(Some(value), Some(Map[String, Any]("time" -> new Date())))
  }

  def exceptionStep(pipelineContext: PipelineContext): PipelineStepResponse = {
    throw PipelineException(message = Some("Called exception step"),
      pipelineId = pipelineContext.getGlobalString("pipelineId"),
      stepId = pipelineContext.getGlobalString("stepId"))
  }

  def pauseStep(pipelineContext: PipelineContext): Unit = {
    pipelineContext.addStepMessage(
      PipelineStepMessage("Called pause step",
        pipelineContext.getGlobalString("stepId").getOrElse(""),
        pipelineContext.getGlobalString("pipelineId").getOrElse(""),
        PipelineStepMessageType.pause))
  }
}
