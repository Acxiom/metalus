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

  describe("DefaultPipelineDriver") {
    it("Should run a basic pipeline") {
      val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value")
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
          step.id.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              pipelineContext.parameters.getParametersByPipelineId("1").get.parameters("GLOBALVALUESTEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case _ =>
          }
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case pe: PauseException =>
              assert(pe.pipelineId.getOrElse("") == "1")
              assert(pe.stepId.getOrElse("") == "PAUSESTEP")
          }
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
    }

    it("Should run two pipelines") {
      val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "two",
        "--globalInput", "global-input-value")
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
          step.id.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              pipelineContext.parameters.getParametersByPipelineId(pipeline.id.getOrElse("-1")).get.parameters("GLOBALVALUESTEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case _ => fail("Unexpected pipeline finished")
          }
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ => fail("Unexpected exception registered")
          }
        }

        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
          assert(pipelines.lengthCompare(2) == 0)
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
    }

    it("Should run one pipeline and pause") {
      val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "three",
        "--globalInput", "global-input-value")
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
          step.id.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              pipelineContext.parameters.getParametersByPipelineId(pipeline.id.getOrElse("-1")).get.parameters("GLOBALVALUESTEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case _ =>
          }
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case pe: PauseException =>
              assert(pe.pipelineId.getOrElse("") == "0")
              assert(pe.stepId.getOrElse("") == "PAUSESTEP")
          }
        }

        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
          assert(pipelines.lengthCompare(1) == 0)
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
    }

    it("Should run second step because first returns nothing") {
      val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "four",
        "--globalInput", "global-input-value")
      SparkTestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
          step.id.getOrElse("") match {
            case "DYNAMICBRANCHSTEP" =>
              pipelineContext.parameters.getParametersByPipelineId(pipeline.id.getOrElse("-1")).get.parameters("DYNAMICBRANCHSTEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case "DYNAMICBRANCH2STEP" =>
              pipelineContext.parameters.getParametersByPipelineId(pipeline.id.getOrElse("-1")).get.parameters("DYNAMICBRANCH2STEP")
                .asInstanceOf[PipelineStepResponse].primaryReturn.get.asInstanceOf[String] == "global-input-value"
            case _ =>
          }
          None
        }
        override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          fail("Unexpected exception registered")
        }

        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
          assert(pipelines.lengthCompare(1) == 0)
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
    }
  }

  it("Should accept changes to pipelineContext at the before processing a step") {
    val args = List("--driverSetupClass", "com.acxiom.pipeline.SparkTestDriverSetup", "--pipeline", "nopause",
      "--globalInput", "global-input-value")
    SparkTestHelper.pipelineListener = new PipelineListener {
      override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
        assert(!pipelineContext.parameters.getParametersByPipelineId("1").get.parameters.contains("madeit"))
        step.id.getOrElse("") match {
          case "GLOBALVALUESTEP" =>
            Some(pipelineContext.copy(parameters = pipelineContext.parameters.copy(
              pipelineContext.parameters.parameters :+ PipelineParameter ("madeit", Map ())
            )))
            None
          case _ => None
        }
      }

      override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
        assert(!pipelineContext.parameters.getParametersByPipelineId("1").get.parameters.contains("madeit"))
        None
      }
    }
    // Execution should complete without exception
    DefaultPipelineDriver.main(args.toArray)
  }
}

object SparkTestHelper {
  val MASTER = "local[2]"
  val APPNAME = "file-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineListener: PipelineListener = _
}

case class SparkTestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  override def pipelines: List[Pipeline] = parameters("pipeline") match {
    case "basic" => PipelineDefs.BASIC_PIPELINE
    case "two" => PipelineDefs.TWO_PIPELINE
    case "three" => PipelineDefs.THREE_PIPELINE
    case "four" => PipelineDefs.FOUR_PIPELINE
    case "nopause" => PipelineDefs.BASIC_NOPAUSE
  }

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = {
    PipelineContext(Some(SparkTestHelper.sparkConf), Some(SparkTestHelper.sparkSession), Some(parameters),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
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

  def returnNothingStep(string: String): Unit = {}
}
