package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations._
import com.acxiom.metalus.{PipelineContext, PipelineStepResponse}
import org.slf4j.{Logger, LoggerFactory}

@StepObject
object FlowUtilsSteps {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  @StepFunction("cc8d44ad-5049-460f-87c4-e250b9fa53f1",
    "Empty Check",
    "Determines if the provided value is defined. Returns true if the value is not defined.",
    "branch", "Utilities", List[String]("batch"))
  @BranchResults(List("true", "false"))
  def isEmpty(value: Any): Boolean = {
    value match {
      case o: Option[_] => o.isEmpty
      case _ => Option(value).isEmpty
    }
  }

  @StepFunction("6ed36f89-35d1-4280-a555-fbcd8dd76bf2",
    "Retry (simple)",
    "Makes a decision to retry or stop based on a named counter",
    "branch", "RetryLogic", List[String]("batch"))
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
