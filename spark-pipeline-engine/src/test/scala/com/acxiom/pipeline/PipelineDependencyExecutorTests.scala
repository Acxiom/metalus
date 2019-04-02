package com.acxiom.pipeline

import java.io.File
import java.util.Date

import com.acxiom.pipeline.applications.ApplicationUtils
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen, Suite}

import scala.collection.mutable
import scala.io.Source

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

  private val pipelineJson =
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

  describe("Execution Plan") {
    it("Should execute a single list of pipelines") {
      val results = new ListenerValidations
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          results.addValidation("Execution failed", getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Fred")
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              results.addValidation("Unexpected exception registered", valid = false)
          }
        }
      }
      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/single-execution.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()
    }

    it("Should execute a simple dependency graph of two pipeline chains") {
      val results = new ListenerValidations
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              results.addValidation("Execution failed", getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Fred")
            case "Pipeline2" =>
              results.addValidation("Execution failed", getStringValue(pipelineContext, "Pipeline2", "Pipeline2Step1") == "Fred")
          }
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              results.addValidation("Unexpected exception registered", valid = false)
          }
        }
      }
      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/parent-child.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()
    }

    it("Should execute a multi-tiered chain of dependencies") {
      val results = new ListenerValidations
      val resultBuffer = mutable.ListBuffer[String]()
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain1", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain1", "Pipeline3Step2") == "Chain1")
            case "PipelineChain2" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "Pipeline3", "Pipeline3Step1") == "Chain1" &&
                  getStringValue(pipelineContext, "Pipeline3", "Pipeline3Step2") == "Chain3")
            case "PipelineChain5" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(s"Failed: ${e.getMessage}", valid = false)
          }
        }
      }

      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/multi-tiered.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()

      val validResults = List("Pipeline1", "PipelineChain1", "PipelineChain2", "PipelineChain3", "PipelineChain5", "Pipeline3")
      assert(resultBuffer.diff(validResults).isEmpty)
    }

    it("Should not execute child when one parent fails with an exception") {
      val results = new ListenerValidations
      val resultBuffer = mutable.ListBuffer[String]()
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              results.addValidation("Should not have called PipelineChain1", valid = false)
            case "PipelineChain2" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              results.addValidation("Should not have called Pipeline3", valid = false)
            case "PipelineChain5" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case ex: PipelineException =>
              if (ex.message.getOrElse("") != "Called exception step") {
                val e = Option(exception.getCause).getOrElse(exception)
                results.addValidation(s"Failed: ${e.getMessage}", valid = false)
              }
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(s"Failed: ${e.getMessage}", valid = false)
          }
        }
      }

      // PipelineChain1 should throw an exception which h=should prevent Pipeline3 from executing
      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/multi-tiered-exception.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()

      val validResults = List("Pipeline1", "PipelineChain2", "PipelineChain3", "PipelineChain5")
      assert(resultBuffer.diff(validResults).isEmpty)
    }

    it("Should not execute child when one parent pauses") {
      val results = new ListenerValidations
      val resultBuffer = mutable.ListBuffer[String]()
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          var pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline1" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "Pipeline1", "Pipeline1Step1") == "Chain0")
            case "PipelineChain1" =>
              results.addValidation("Should not have called PipelineChain1", valid = false)
            case "PipelineChain2" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain2", "Pipeline3Step2") == "Chain2")
            case "PipelineChain3" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step1") == "Chain0" &&
                  getStringValue(pipelineContext, "PipelineChain3", "Pipeline3Step2") == "Chain3")
            case "Pipeline3" =>
              results.addValidation("Should not have called Pipeline3", valid = false)
            case "PipelineChain5" =>
              results.addValidation(s"Execution failed for $pipelineId",
                getStringValue(pipelineContext, "PipelineChain5", "Pipeline1Step1") == "Chain5")
          }
          resultBuffer += pipelineId
          None
        }

        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case ex: PauseException =>
              if (ex.message.getOrElse("") != "Called pause step") {
                val e = Option(exception.getCause).getOrElse(exception)
                results.addValidation(s"Failed: ${e.getMessage}", valid = false)
              }
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(s"Failed: ${e.getMessage}", valid = false)
          }
        }
      }

      // PipelineChain1 should throw an exception which h=should prevent Pipeline3 from executing
      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/multi-tiered-pause.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()

      val validResults = List("Pipeline1", "PipelineChain2", "PipelineChain3", "PipelineChain5")
      assert(resultBuffer.diff(validResults).isEmpty)
    }
  }

  describe("Negative Tests") {
    it("Should not execute if there is no root execution") {
      var processExecuted = false
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
          processExecuted = true
          None
        }

        override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          processExecuted = true
          None
        }
      }
      PipelineDependencyExecutor.executePlan(List(PipelineExecution("Fred",
        DriverUtils.parsePipelineJson(pipelineJson.replace("$value", "Fred")).get,
        None, generatePipelineContext(), Some(List("meeso")))))
      assert(!processExecuted)
    }
  }

  private def getStringValue(pipelineContext: PipelineContext, pipelineId: String, stepId: String): String = {
    val pipelineParameters = pipelineContext.parameters.getParametersByPipelineId(pipelineId)
    if (pipelineParameters.isDefined) {
      val response = pipelineParameters.get.parameters(stepId).asInstanceOf[PipelineStepResponse]
      response.primaryReturn.getOrElse("").asInstanceOf[String]
    } else {
      ""
    }
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

  def exceptionStep(value: String, pipelineContext: PipelineContext): PipelineStepResponse = {
    throw PipelineException(message = Some("Called exception step"),
      pipelineId = pipelineContext.getGlobalString("pipelineId"),
      stepId = pipelineContext.getGlobalString("stepId"))
  }

  def pauseStep(value: String, pipelineContext: PipelineContext): Unit = {
    pipelineContext.addStepMessage(
      PipelineStepMessage("Called pause step",
        pipelineContext.getGlobalString("stepId").getOrElse(""),
        pipelineContext.getGlobalString("pipelineId").getOrElse(""),
        PipelineStepMessageType.pause))
  }
}
