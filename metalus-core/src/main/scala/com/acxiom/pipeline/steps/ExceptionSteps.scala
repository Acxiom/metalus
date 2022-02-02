package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.{PipelineContext, PipelineException, SkipExecutionPipelineStepException}

@StepObject
object ExceptionSteps {

  @StepFunction("403b1b7e-13d1-4e28-856a-6c1185442b2c",
    "Skip Exception",
    "Throws an Exception that will indicate that the current execution should be skipped. This exception is intended to be used in evaluation pipelines only.",
    "Pipeline",
    "Exceptions")
  @StepParameters(Map("message" -> StepParameter(Some("message"), Some(true), None,
    None, None, None, Some("Thee message to log when the exception is thrown"))))
  def throwSkipExecutionException(message: String, pipelineContext: PipelineContext): Unit = {
    throw SkipExecutionPipelineStepException(message = Some(message), context = Some(pipelineContext))
  }

  @StepFunction("fb6c6293-c51d-49ab-a77e-de389610cdd6c",
    "Pipeline Exception",
    "Throws an Exception that will indicate that the current pipeline should stop.",
    "Pipeline",
    "Exceptions")
  @StepParameters(Map("message" -> StepParameter(Some("message"), Some(true), None,
    None, None, None, Some("Thee message to log when the exception is thrown"))))
  def throwPipelineException(message: String, pipelineContext: PipelineContext): Unit = {
    throw PipelineException(message = Some(message),
      context = Some(pipelineContext),
      pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
  }
}
