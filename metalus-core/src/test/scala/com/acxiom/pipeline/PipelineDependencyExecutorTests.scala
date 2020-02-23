package com.acxiom.pipeline

import java.io.File
import java.util.Date

import com.acxiom.pipeline.applications.ApplicationUtils
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.collection.mutable
import scala.io.Source

class PipelineDependencyExecutorTests extends FunSpec with BeforeAndAfterAll with Suite {

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
              results.addValidation(s"Pipeline Audit missing for $pipelineId", pipelineContext.getPipelineAudit(pipelineId).isDefined)
              results.addValidation(s"Step Audits missing for $pipelineId", pipelineContext.getPipelineAudit(pipelineId).get.children.isDefined)
              results.addValidation(s"Step Audits missing for $pipelineId", pipelineContext.getPipelineAudit(pipelineId).get.children.get.length == 2)
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

      // PipelineChain1 should throw an exception which should prevent Pipeline3 from executing
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

    it("Should handle GlobalLinks") {
      val results = new ListenerValidations
      val listener = new PipelineListener {
        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
          pipelineId match {
            case "Pipeline0" =>
              // shared parameter is overridden with globallink
              results.addValidation(s"Execution failed for shared GlobalLink in Pipeline0",
                getStringValue(pipelineContext, "Pipeline0", "Pipeline0StepA") == "pipeline0_override")
            case "Pipeline1" =>  // parent is 0
              // shared parameter is overridden with globallink
              results.addValidation(s"Execution failed for shared GlobalLink in Pipeline1",
                getStringValue(pipelineContext, "Pipeline1", "Pipeline1StepA") == "pipeline1_override")
              // able to access parent global link
              results.addValidation(s"Execution failed for Pipeline0 GlobalLink on Pipeline1",
                getStringValue(pipelineContext, "Pipeline1", "Pipeline1StepB") == "pipeline0_original")
            case "Pipeline2" =>  // parent is 0
              // not overriding shared global link, so it reverts to how the parent set it
              results.addValidation(s"Execution failed for shared GlobalLink in Pipeline2",
                getStringValue(pipelineContext, "Pipeline2", "Pipeline2StepA") == "pipeline0_override")
              // able to access parent global link
              results.addValidation(s"Execution failed for Pipeline0 GlobalLink on Pipeline2",
                getStringValue(pipelineContext, "Pipeline2", "Pipeline2StepB") == "pipeline0_original")
            case "Pipeline3" =>  // parent is 0, 1
              // shared parameter is taken from 1 since it overrode 0
              results.addValidation(s"Execution failed for shared GlobalLink in Pipeline3",
                getStringValue(pipelineContext, "Pipeline3", "Pipeline3StepA") == "pipeline1_override")
              // able to access grand-parent global link
              results.addValidation(s"Execution failed for Pipeline0 GlobalLink on Pipeline3",
                getStringValue(pipelineContext, "Pipeline3", "Pipeline3StepB") == "pipeline0_original")
              // able to access parent global link
              results.addValidation(s"Execution failed for Pipeline1 GlobalLink on Pipeline3",
                getStringValue(pipelineContext, "Pipeline3", "Pipeline3StepC") == "pipeline1_original")
            case "Pipeline4" =>  // parent is 0, 2
              // shared parameter is overridden with global link
              results.addValidation(s"Execution failed for shared GlobalLink in Pipeline4",
                getStringValue(pipelineContext, "Pipeline4", "Pipeline4StepA") == "pipeline4_override")
              // able to access grand-parent global link
              results.addValidation(s"Execution failed for Pipeline0 GlobalLink on Pipeline4",
                getStringValue(pipelineContext, "Pipeline4", "Pipeline4StepB") == "pipeline0_original")
              // unable to access global link from pipeline1
              results.addValidation(s"Execution failed for Pipeline1 GlobalLink on Pipeline4",
                getStringValue(pipelineContext, "Pipeline4", "Pipeline4StepC") == "unknown")
          }
          None
        }
      }

      val application = ApplicationUtils.parseApplication(Source.fromInputStream(getClass.getResourceAsStream("/global-links.json")).mkString)
      PipelineDependencyExecutor.executePlan(ApplicationUtils.createExecutionPlan(application, None, SparkTestHelper.sparkConf, listener))
      results.validate()
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
        None, SparkTestHelper.generatePipelineContext(), Some(List("meeso")))))
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

  def sleepFunction(value: String, pipelineContext: PipelineContext): PipelineStepResponse = {
    Thread.sleep(ONE_SEC)
    PipelineStepResponse(Some(value), Some(Map[String, Any]("time" -> new Date())))
  }

  def normalFunction(value: String, pipelineContext: PipelineContext): PipelineStepResponse = {
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
