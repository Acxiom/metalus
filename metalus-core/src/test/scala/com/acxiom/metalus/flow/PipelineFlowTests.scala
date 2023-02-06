package com.acxiom.metalus.flow

import com.acxiom.metalus.applications.ApplicationUtils
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.{Constants, PipelineExecutor, TestHelper}
import org.scalatest.funspec.AnyFunSpec

import scala.io.Source

class PipelineFlowTests extends AnyFunSpec {
  describe("Simple pipeline") {
    it("should use an alternate step") {
      val settings = TestHelper.setupTestDB("alternateStepTest")
      val application = JsonParser.parseApplication(
        Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/simple_restart_application.json")).mkString)
      val credentialProvider = TestHelper.getDefaultCredentialProvider
      val pipelineListener = RestartPipelineListener()
      val pipelineContext = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
          "connectionString" -> settings.url,
          "step2Value" -> "first run")), Some(Map[String, Any]("executionEngines" -> "test")),
        pipelineListener, Some(credentialProvider))

      assert(pipelineContext.executionEngines.isDefined)
      assert(pipelineContext.executionEngines.get.length == Constants.TWO)
      assert(pipelineContext.executionEngines.get.head == "test")

      val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
      assert(result.success)
      assert(pipelineListener.getStepList.nonEmpty)
      assert(pipelineListener.getStepList.length == 3)
      val step2Result = result.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_2")
      assert(step2Result.isDefined)
      assert(step2Result.get.primaryReturn.get.toString == "ALT_STEP1 -> STEP2 -> first run")
      val step3Result = result.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_3")
      assert(step3Result.isDefined)
      assert(step3Result.get.primaryReturn.get.toString == "ALT_STEP1 -> STEP2 -> first run -> STEP3")

      TestHelper.stopTestDB(settings.name)
    }
  }
}
