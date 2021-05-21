package com.acxiom.pipeline.flow

import com.acxiom.pipeline.utils.DriverUtils
import com.acxiom.pipeline.{PipelineExecutor, PipelineListener, PipelineStepResponse, SparkTestHelper}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.File
import scala.io.Source

class SplitMergeStepTests extends FunSpec with BeforeAndAfterAll with Suite {
  private val simpleSplitPipeline =
    DriverUtils.parsePipelineJson(
      Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/simple_split_flow.json")).mkString).get.head
  private val complexSplitPipeline =
    DriverUtils.parsePipelineJson(
      Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/complex_split_flow.json")).mkString).get.head

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

  describe("Simple Flow Validation") {
    it("Should fail when less than two flows are part of the split step") {
      val pipeline =
        DriverUtils.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_split_flow.json")).mkString).get.head
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "At least two paths need to be defined to use a split step!")
    }

    it("Should fail when step ids are not unique across splits") {
      val pipeline =
        DriverUtils.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_step_split_flow.json")).mkString).get.head
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Step Ids must be unique across split flows!")
    }

    it("Should process simple split step pipeline") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(simpleSplitPipeline), None, SparkTestHelper.generatePipelineContext())
      assert(executionResult.success)
      assert(executionResult.pipelineContext.parameters.parameters.length == 1)
      val parameters = executionResult.pipelineContext.parameters.getParametersByPipelineId(simpleSplitPipeline.id.get)
      assert(parameters.isDefined)
      assert(parameters.get.parameters.contains("FORMAT_STRING"))
      assert(parameters.get.parameters("FORMAT_STRING").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(parameters.get.parameters("FORMAT_STRING").asInstanceOf[PipelineStepResponse]
        .primaryReturn.get.asInstanceOf[String] == "List with values 1,2 has a sum of 3")
      // Verify audits
      val rootAudit = executionResult.pipelineContext.rootAudit
      assert(rootAudit.children.isDefined && rootAudit.children.get.length == 1)
      val pipelineAudit = rootAudit.children.get.head
      assert(pipelineAudit.children.isDefined && pipelineAudit.children.get.length == 6)
      val childAudits = pipelineAudit.children.get
      assert(childAudits.exists(_.id == "SPLIT"))
      assert(childAudits.exists(_.id == "MERGE"))
      assert(childAudits.exists(_.id == "FORMAT_STRING"))
      assert(childAudits.exists(_.id == "STRING_VALUES"))
      assert(childAudits.exists(_.id == "GENERATE_DATA"))
      assert(childAudits.exists(_.id == "SUM_VALUES"))
    }
  }

  describe("Complex Flow Validation") {
    it("Should fail if more than 2 logical paths are generated") {
      val pipeline =
        DriverUtils.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_complex_split_flow.json")).mkString).get.head
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Flows must either terminate at a single merge step or pipeline end!")
    }

    it("Should process complex split step pipeline") {
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(complexSplitPipeline), None, SparkTestHelper.generatePipelineContext())
      assert(executionResult.success)
      assert(executionResult.pipelineContext.parameters.parameters.length == 1)
      val parameters = executionResult.pipelineContext.parameters.getParametersByPipelineId(complexSplitPipeline.id.get)
      assert(parameters.isDefined)
      assert(parameters.get.parameters.contains("FORMAT_STRING"))
      assert(parameters.get.parameters("FORMAT_STRING").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(parameters.get.parameters("FORMAT_STRING").asInstanceOf[PipelineStepResponse]
        .primaryReturn.get.asInstanceOf[String] == "List with values 1,2 has a sum of 3")
      assert(parameters.get.parameters.contains("FORMAT_STRING_PART_2"))
      assert(parameters.get.parameters("FORMAT_STRING_PART_2").asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(parameters.get.parameters("FORMAT_STRING_PART_2").asInstanceOf[PipelineStepResponse]
        .primaryReturn.get.asInstanceOf[String] == "List has a sum of 3")
      // Verify audits
      val rootAudit = executionResult.pipelineContext.rootAudit
      assert(rootAudit.children.isDefined && rootAudit.children.get.length == 1)
      val pipelineAudit = rootAudit.children.get.head
      assert(pipelineAudit.children.isDefined && pipelineAudit.children.get.length == 9)
      val childAudits = pipelineAudit.children.get
      assert(childAudits.exists(_.id == "SPLIT"))
      assert(childAudits.exists(_.id == "MERGE"))
      assert(childAudits.exists(_.id == "FORMAT_STRING"))
      assert(childAudits.exists(_.id == "FORMAT_STRING_PART_2"))
      assert(childAudits.exists(_.id == "STRING_VALUES"))
      assert(childAudits.exists(_.id == "GENERATE_DATA"))
      assert(childAudits.exists(_.id == "SUM_VALUES"))
      assert(childAudits.exists(_.id == "SUM_VALUES_NOT_MERGED"))
      assert(childAudits.exists(_.id == "BRANCH"))
    }
  }

  describe("Flow Error Handling") {
    it("Should fail when an error is encountered") {
      val message =
        """One or more errors has occurred while processing split step:
          | Split Step SUM_VALUES: exception thrown for string value (some other value)
          | Split Step STRING_VALUES: exception thrown for string value (doesn't matter)
          |""".stripMargin
      val pipeline =
        DriverUtils.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/error_split_flow.json")).mkString).get.head
      SparkTestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(List(pipeline), None, SparkTestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == message)
    }
  }
}
