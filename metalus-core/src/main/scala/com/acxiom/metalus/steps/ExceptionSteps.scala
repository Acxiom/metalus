package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.metalus.{PipelineContext, PipelineException, SkipExecutionPipelineStepException}

@StepObject
object ExceptionSteps {

  @StepFunction("403b1b7e-13d1-4e28-856a-6c1185442b2c",
    "Skip Exception",
    "Throws an Exception that will indicate that the current execution should be skipped. This exception is intended to be used in evaluation pipelines only.",
    "Pipeline",
    "Exceptions")
  @StepParameters(Map("message" -> StepParameter(Some("message"), Some(true), None,
    None, None, None, Some("The message to log when the exception is thrown"))))
  def throwSkipExecutionException(message: String, pipelineContext: PipelineContext): Unit = {
    throw SkipExecutionPipelineStepException(message = Some(message), context = Some(pipelineContext))
  }

  @StepFunction("fb6c6293-c51d-49ab-a77e-de389610cdd6c",
    "Pipeline Exception",
    "Throws an Exception that will indicate that the current pipeline should stop.",
    "Pipeline",
    "Exceptions")
  @StepParameters(Map("message" -> StepParameter(Some("message"), Some(true), None, None, None, None, Some("The message to log when the exception is thrown")),
  "cause" -> StepParameter(Some("cause"), Some(false), None, None, None, None, Some("An optional exception to include in the thrown exception")),
  "stepIdOverride" -> StepParameter(Some("stepIdOverride"), Some(false), None, None, None, None, Some("An optional stepId to use instead of the default"))))
  def throwPipelineException(message: String, cause: Option[Throwable] = None,
                             stepIdOverride: Option[String] = None, pipelineContext: PipelineContext): Unit = {
    val progress = stepIdOverride.map(id => pipelineContext.currentStateInfo.get.copy(stepId = Some(id)))
      .getOrElse(pipelineContext.currentStateInfo.get)
    throw PipelineException(message = Some(message), context = Some(pipelineContext),
      cause = cause.orNull, pipelineProgress = Some(progress))
  }
}
