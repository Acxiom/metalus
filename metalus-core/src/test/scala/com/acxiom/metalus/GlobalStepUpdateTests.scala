package com.acxiom.metalus

import com.acxiom.metalus.parser.JsonParser
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.event.Level

class GlobalStepUpdateTests extends AnyFunSpec with BeforeAndAfterAll with Suite {
  override def beforeAll() {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)

    TestHelper.pipelineListener = PipelineListener()
  }

  override def afterAll() {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("Global Value Manipulation from simple Steps") {
    val pipelineJson =
    """
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
      |         "command": "MockStepObject.mockStepSetGlobal"
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
      |         "command": "MockStepObject.mockStepSetGlobal"
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
      |         "command": "MockStepObject.mockStepFunctionAnyResponse"
      |       }
      |     }
      |   ]
      | }
    """.stripMargin
    it("Should allow steps to add a global") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == Constants.THREE)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred")
      assert(ctx.globals.get.contains("redonthehead1"))
      assert(ctx.globals.get("redonthehead1") == "fred1")
    }

    it("Should allow steps to overwrite a global") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson.replaceAll("redonthehead1", "redonthehead"))
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == Constants.TWO)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred1")
    }

    it("Should set the last step id as a global") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == Constants.THREE)
      assert(ctx.globals.get.contains("lastStepId"))
      assert(ctx.globals.get("lastStepId") == "Pipeline1.Pipeline1Step3")
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
        |           "className": "com.acxiom.metalus.Pipeline",
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
        |         "command": "MockStepObject.mockStepSetGlobal"
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
        |         "command": "MockStepObject.mockStepSetGlobal"
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
        |         "command": "MockStepObject.mockStepFunctionAnyResponse"
        |       }
        |     }
        |   ]
        |           }
        |         }
        |       ],
        |       "engineMeta": {
        |         "command": "MockStepObject.mockStepSetGlobal"
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
        |         "command": "MockStepObject.mockStepSetGlobal"
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
        |         "command": "MockStepObject.mockStepFunctionAnyResponse"
        |       }
        |     }
        |   ]
        | }
        |]
    """.stripMargin
    it("Should allow steps to add a global") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == Constants.FOUR)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred")
      assert(ctx.globals.get.contains("redonthehead1"))
      assert(ctx.globals.get("redonthehead1") == "fred1")
      assert(ctx.globals.get.contains("redonthehead2"))
      assert(ctx.globals.get("redonthehead2") == "fred2")
    }

    it("Should allow steps to overwrite a global") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson.replaceAll("redonthehead1", "redonthehead"))
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      assert(ctx.globals.isDefined)
      assert(ctx.globals.get.size == Constants.THREE)
      assert(ctx.globals.get.contains("redonthehead"))
      assert(ctx.globals.get("redonthehead") == "fred1")
      assert(ctx.globals.get.contains("redonthehead2"))
      assert(ctx.globals.get("redonthehead2") == "fred2")
    }
  }

  describe("Metric Value Manipulation") {
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
        |         "command": "MockStepObject.mockStepSetGlobal"
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
        |         "command": "MockStepObject.mockStepSetGlobal"
        |       }
        |     },
        |     {
        |       "id": "Pipeline1Step3",
        |       "displayName": "Pipeline1Step1",
        |       "type": "pipeline",
        |       "nextStepId": "Pipeline1Step4",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "fred1"
        |         }
        |       ],
        |       "engineMeta": {
        |         "command": "MockStepObject.mockStepFunctionAnyResponse"
        |       }
        |     },
        |     {
        |       "id": "Pipeline1Step4",
        |       "displayName": "Pipeline1Step4",
        |       "type": "pipeline",
        |       "params": [
        |         {
        |           "type": "text",
        |           "name": "string",
        |           "required": true,
        |           "value": "2"
        |         },
        |         {
        |           "type": "text",
        |           "name": "metricName",
        |           "required": true,
        |           "value": "chickenCount"
        |         }
        |       ],
        |       "engineMeta": {
        |         "command": "MockStepObject.mockStepSetMetric"
        |       }
        |     }
        |   ]
        | }
        |]
    """.stripMargin
    it("Should allow steps to add metrics") {
      val context = TestHelper.generatePipelineContext()
      val pipelines = JsonParser.parsePipelineJson(pipelineJson)
      val result = PipelineExecutor.executePipelines(pipelines.get.head, context)
      assert(result.success)
      val ctx = result.pipelineContext
      val pipelineKey = PipelineStateInfo("Pipeline1", Some("Pipeline1Step4"))
      val stepAudit = ctx.getPipelineAudit(pipelineKey)
      assert(stepAudit.isDefined)
      val metric = stepAudit.get.getMetric("chickenCount")
      assert(metric.isDefined)
      assert(metric.get.isInstanceOf[String] && metric.get.asInstanceOf[String] == "2")
    }
  }
}
