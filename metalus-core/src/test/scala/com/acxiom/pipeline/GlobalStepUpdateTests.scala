package com.acxiom.pipeline

import java.io.File

import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

class GlobalStepUpdateTests extends FunSpec with BeforeAndAfterAll with Suite {
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

    SparkTestHelper.pipelineListener = PipelineListener()
  }

  override def afterAll() {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  describe("Global Value Manipulation from simple Steps") {
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
      |       "type": "pipeline",
      |       "nextStepId": "Pipeline1Step2",
      |       "params": [
      |         {
      |           "type": "text",
      |           "name": "string",
      |           "required": true,
      |           "value": "fred"
      |         },
      |         {
      |           "type": "text",
      |           "name": "globalName",
      |           "required": true,
      |           "value": "redonthehead"
      |         }
      |       ],
      |       "engineMeta": {
      |         "spark": "MockStepObject.mockStepSetGlobal"
      |       }
      |     },
      |     {
      |       "id": "Pipeline1Step2",
      |       "displayName": "Pipeline1Step1",
      |       "type": "pipeline",
      |       "nextStepId": "Pipeline1Step3",
      |       "params": [
      |         {
      |           "type": "text",
      |           "name": "string",
      |           "required": true,
      |           "value": "fred1"
      |         },
      |         {
      |           "type": "text",
      |           "name": "globalName",
      |           "required": true,
      |           "value": "redonthehead1"
      |         }
      |       ],
      |       "engineMeta": {
      |         "spark": "MockStepObject.mockStepSetGlobal"
      |       }
      |     },
      |     {
      |       "id": "Pipeline1Step3",
      |       "displayName": "Pipeline1Step1",
      |       "type": "pipeline",
      |       "params": [
      |         {
      |           "type": "text",
      |           "name": "string",
      |           "required": true,
      |           "value": "fred1"
      |         }
      |       ],
      |       "engineMeta": {
      |         "spark": "MockStepObject.mockStepFunctionAnyResponse"
      |       }
      |     }
      |   ]
      | }
      |]
    """.stripMargin
    it("Should allow steps to add a global") {
      val context = SparkTestHelper.generatePipelineContext()
      val pipelines = DriverUtils.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get, None, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == 4)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred")
      assert(ctx.globals.get.contains("redonthehead1"))
      assert(ctx.globals.get("redonthehead1") == "fred1")
    }

    it("Should allow steps to overwrite a global") {
      val context = SparkTestHelper.generatePipelineContext()
      val pipelines = DriverUtils.parsePipelineJson(pipelineJson.replaceAll("redonthehead1", "redonthehead"))
      val result = PipelineExecutor.executePipelines(pipelines.get, None, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == 3)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred1")
    }
  }

  describe("Global Value Manipulation from step groups") {
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
        |       "type": "step-group",
        |       "nextStepId": "Pipeline1Step2",
        |       "params": [
        |         {
        |           "type": "object",
        |           "className": "com.acxiom.pipeline.DefaultPipeline",
        |           "name": "pipeline",
        |           "required": true,
        |           "value": {
        |             "id": "Pipeline1",
        |   "name": "Sub Pipeline 1",
        |   "category": "step-group",
        |   "steps": [
        |     {
        |       "id": "SubPipeline1Step1",
        |       "displayName": "SubPipeline1Step1",
        |       "type": "pipeline",
        |       "nextStepId": "SubPipeline1Step2",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred"
        |         },
        |         {
        |           "type": "text",
        |           "name": "globalName",
        |           "required": true,
        |           "value": "redonthehead"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepSetGlobal"
        |       }
        |     },
        |     {
        |       "id": "SubPipeline1Step2",
        |       "displayName": "SubPipeline1Step1",
        |       "type": "pipeline",
        |       "nextStepId": "SubPipeline1Step3",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred1"
        |         },
        |         {
        |           "type": "text",
        |           "name": "globalName",
        |           "required": true,
        |           "value": "redonthehead1"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepSetGlobal"
        |       }
        |     },
        |     {
        |       "id": "SubPipeline1Step3",
        |       "displayName": "SubPipeline1Step1",
        |       "type": "pipeline",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred1"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepFunctionAnyResponse"
        |       }
        |     }
        |   ]
        |           }
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepSetGlobal"
        |       }
        |     },
        |     {
        |       "id": "Pipeline1Step2",
        |       "displayName": "Pipeline1Step1",
        |       "type": "pipeline",
        |       "nextStepId": "Pipeline1Step3",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred2"
        |         },
        |         {
        |           "type": "text",
        |           "name": "globalName",
        |           "required": true,
        |           "value": "redonthehead2"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepSetGlobal"
        |       }
        |     },
        |     {
        |       "id": "Pipeline1Step3",
        |       "displayName": "Pipeline1Step1",
        |       "type": "pipeline",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred1"
        |         }
        |       ],
        |       "engineMeta": {
        |         "spark": "MockStepObject.mockStepFunctionAnyResponse"
        |       }
        |     }
        |   ]
        | }
        |]
    """.stripMargin
    it("Should allow steps to add a global") {
      val context = SparkTestHelper.generatePipelineContext()
      val pipelines = DriverUtils.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get, None, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == 5)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred")
      assert(ctx.globals.get.contains("redonthehead1"))
      assert(ctx.globals.get("redonthehead1") == "fred1")
      assert(ctx.globals.get.contains("redonthehead2"))
      assert(ctx.globals.get("redonthehead2") == "fred2")
    }

    it("Should allow steps to overwrite a global") {
      val context = SparkTestHelper.generatePipelineContext()
      val pipelines = DriverUtils.parsePipelineJson(pipelineJson.replaceAll("redonthehead1", "redonthehead"))
      val result = PipelineExecutor.executePipelines(pipelines.get, None, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == 4)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred1")
      assert(ctx.globals.get.contains("redonthehead2"))
      assert(ctx.globals.get("redonthehead2") == "fred2")
    }
  }
}
