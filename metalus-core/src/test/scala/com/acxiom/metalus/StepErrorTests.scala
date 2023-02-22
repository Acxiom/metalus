package com.acxiom.metalus

import org.scalatest.funspec.AnyFunSpec

class StepErrorTests extends AnyFunSpec {
  describe("StepErrorHandling - Basic") {
    val stepToThrowError = PipelineStep(Some("PROCESS_RAW_VALUE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("RAW_DATA")))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockExceptionStepFunction"))), nextStepOnError = Some("HANDLE_ERROR"))
    val errorHandlingStep = PipelineStep(Some("HANDLE_ERROR"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("ex"), value = Some("@LastStepId")))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.errorHandlingStep"))))

    it("Should move execute nextStepOnError") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"), Some(List(stepToThrowError, errorHandlingStep)))
      TestHelper.pipelineListener = PipelineListener()
      val context = TestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(pipeline, context)
      assert(executionResult.success)
      val res = executionResult.pipelineContext.getStepResultByStateInfo(PipelineStateKey("Simple_error_test", Some("HANDLE_ERROR")))
      assert(res.isDefined)
      assert(res.get.primaryReturn.get == "An unknown exception has occurred")
    }

    it("Should fail if an exception is thrown and nextStepOnError is not set") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"),
        Some(List(stepToThrowError.copy(nextStepOnError = None), errorHandlingStep)))
      TestHelper.pipelineListener = PipelineListener()
      val context = TestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(pipeline, context)
      assert(!executionResult.success)
    }

    it("Should fail if an exception is thrown and nextStepOnError is set to a non-existent step") {
      val pipeline = Pipeline(Some("Simple_error_test"), Some("Simple_error_test"),
        Some(List(stepToThrowError.copy(nextStepOnError = Some("not_here")), errorHandlingStep)))
      TestHelper.pipelineListener = PipelineListener()
      val context = TestHelper.generatePipelineContext().copy(globals = Some(Map[String, Any]("validateStepParameterTypes" -> true)))
      val executionResult = PipelineExecutor.executePipelines(pipeline, context)
      assert(!executionResult.success)
    }
  }

}