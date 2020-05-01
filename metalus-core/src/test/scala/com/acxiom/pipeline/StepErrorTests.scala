package com.acxiom.pipeline

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

class StepErrorTests extends FunSpec with BeforeAndAfterAll with Suite {

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

  describe("StepErrorHandling - Basic") {
    val stepToThrowError = PipelineStep(Some("PROCESS_RAW_VALUE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("RAW_DATA")))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockExceptionStepFunction"))), nextStepOnError = Some("HANDLE_ERROR"))
    val errorHandlingStep = PipelineStep(Some("HANDLE_ERROR"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("ex"), value = Some("@PROCESS_RAW_VALUE")))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.errorHandlingStep"))))

    it("Should move execute nextStepOnError") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"), Some(List(stepToThrowError, errorHandlingStep)))
      SparkTestHelper.pipelineListener = PipelineListener()
      val context = SparkTestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), Some("STEP_ERROR_HANDLING_TEST"), context)
      assert(executionResult.success)
      val res = executionResult.pipelineContext.parameters.parameters.head.parameters.get("HANDLE_ERROR")
      assert(res.isDefined)
      assert(res.get.asInstanceOf[PipelineStepResponse].primaryReturn.get == "An unknown exception has occurred")
    }

    it("Should fail if an exception is thrown and nextStepOnError is not set") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"),
        Some(List(stepToThrowError.copy(nextStepOnError = None), errorHandlingStep)))
      SparkTestHelper.pipelineListener = PipelineListener()
      val context = SparkTestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), Some("STEP_ERROR_HANDLING_TEST"), context)
      assert(!executionResult.success)
    }

    it("Should fail if an exception is thrown and nextStepOnError is set to a non-existent step") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"),
        Some(List(stepToThrowError.copy(nextStepOnError = Some("not_here")), errorHandlingStep)))
      SparkTestHelper.pipelineListener = PipelineListener()
      val context = SparkTestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), Some("STEP_ERROR_HANDLING_TEST"), context)
      assert(!executionResult.success)
    }
  }

}
