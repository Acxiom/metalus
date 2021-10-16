package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}

@StepObject
object FlowUtilsSteps {
  @StepFunction("6ed36f89-35d1-4280-a555-fbcd8dd76bf2",
    "Retry (simple)",
    "Makes a decision to retry or stop based on a named counter",
    "branch", "RetryLogic")
  @BranchResults(List("retry", "stop"))
  @StepParameters(Map("counterName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the counter to use for tracking")),
    "maxRetries" -> StepParameter(None, Some(true), None, None, None, None, Some("The maximum number of retries allowed"))))
  @StepResults(primaryType = "String", secondaryTypes = Some(Map("$globals.$counterName" -> "Int")))
  def simpleRetry(counterName: String, maxRetries: Int, pipelineContext: PipelineContext): PipelineStepResponse = {
    val currentCounter = pipelineContext.getGlobalAs[Int](counterName)
    val decision = if (currentCounter.getOrElse(0) < maxRetries) {
      "retry"
    } else {
      "stop"
    }
    val updateCounter = if (decision == "retry") {
      currentCounter.getOrElse(0) + 1
    } else {
      currentCounter.getOrElse(0)
    }
    PipelineStepResponse(Some(decision), Some(Map[String, Any](s"$$globals.$counterName" -> updateCounter)))
  }
}
