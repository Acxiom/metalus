package com.acxiom.metalus.steps

import com.acxiom.metalus.{Constants, EngineMeta, Parameter, Pipeline, PipelineExecutor, PipelineListener, PipelineStep, RetryPolicy, TestHelper}
import org.scalatest.funspec.AnyFunSpec

class FlowUtilsStepsTests extends AnyFunSpec {

  val STRING_STEP: PipelineStep = PipelineStep(Some("STRINGSTEP"), Some("String Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("value"), Some(true), None, Some("lowercase")))),
    Some("RETRY"), None, None, None, None, None, None, None, Some(EngineMeta(Some("StringSteps.toUpperCase"))))

  val RETRY_STEP: PipelineStep = PipelineStep(Some("RETRY"), Some("Retry Step"), None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("counterName"), Some(true), None, Some("TEST_RETRY_COUNTER")),
      Parameter(Some("object"), Some("retryPolicy"), Some(true), None,
        Some(Map("maximumRetries" -> Constants.FIVE, "waitTimeMultipliesMS" -> Constants.ONE)),
        className = Some("com.acxiom.metalus.RetryPolicy")),
      Parameter(Some("result"), Some("retry"), Some(true), None, Some("STRINGSTEP")))),
    None, None, None, None, None, None, None, None, Some(EngineMeta(Some("FlowUtilsSteps.simpleRetry"))))

  describe("FlowUtilsSteps") {
    describe("Retry") {
      it("should handle retry") {
        TestHelper.pipelineListener = PipelineListener()
        val initialPipelineContext = TestHelper.generatePipelineContext().setGlobal("testCounter", 0)
        val response = FlowUtilsSteps.simpleRetry("testCounter", RetryPolicy(Some(1)), initialPipelineContext)
        assert(response.primaryReturn.get.toString == "retry")
        val stopResponse = FlowUtilsSteps.simpleRetry("testCounter", RetryPolicy(Some(1)), initialPipelineContext.setGlobal("testCounter", 1))
        assert(stopResponse.primaryReturn.get.toString == "stop")
      }

      it("Should retry and trigger stop") {
        TestHelper.pipelineListener = PipelineListener()
        val pipeline = Pipeline(Some("testPipeline"), Some("retryPipeline"), Some(List(STRING_STEP, RETRY_STEP)))
        val initialPipelineContext = TestHelper.generatePipelineContext()
        val result = PipelineExecutor.executePipelines(pipeline, initialPipelineContext)
        val counter = result.pipelineContext.getGlobalAs[Int]("TEST_RETRY_COUNTER")
        assert(counter.isDefined)
        assert(counter.get == Constants.FIVE)
      }
    }

    describe("isEmpty") {
      it("should determine if object is empty") {
        assert(FlowUtilsSteps.isEmpty(None))
        assert(FlowUtilsSteps.isEmpty(None.orNull))
        assert(!FlowUtilsSteps.isEmpty(Some("string")))
        assert(!FlowUtilsSteps.isEmpty("test"))
      }
    }
  }
}
