package com.acxiom.pipeline

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

class PipelineValidationTests extends FunSpec with BeforeAndAfterAll with Suite {

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

  describe("Pipeline Step Validations") {

    it("Should catch steps without step ids") {
      val pipelineSteps = List(PipelineStep(id = None,
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!result.success)
    }

    it("Should catch steps without engine meta") {
      val pipelineSteps = List(PipelineStep(id = Some("ChickenStepId"),
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = None))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!result.success)
    }

    it("Should catch steps without types") {
      val pipelineSteps = List(PipelineStep(id = Some("ChickenStepId"),
        displayName = None,
        description = None,
        `type` = None,
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!result.success)
      val pipelineStepsBadType = List(PipelineStep(id = Some("ChickenStepBadTypeId"),
        displayName = None,
        description = None,
        `type` = Some("moo"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipelineBadType = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineStepsBadType))
      SparkTestHelper.pipelineListener = PipelineListener()
      val resultBadType = PipelineExecutor.executePipelines(List(pipelineBadType), None, SparkTestHelper.generatePipelineContext())
      assert(!resultBadType.success)
    }

    it("Should prevent the use of lastStepId") {
      val pipelineSteps = List(PipelineStep(id = Some("lastStepId"),
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      SparkTestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!result.success)
    }
  }

}
