package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.streaming.StreamingQueryMonitor
import com.acxiom.pipeline.utils.ReflectionUtils
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Date

@StepObject
object FlowUtilsSteps {
  val logger: Logger = Logger.getLogger(getClass)

  @StepFunction("cc8d44ad-5049-460f-87c4-e250b9fa53f1",
    "Empty Check",
    "Determines if the provided value is defined. Returns true if the value is not defined.",
    "branch", "Utilities")
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

  @StepFunction("64c983e2-5eac-4fb6-87b2-024b69aa0ded",
    "Streaming Monitor",
    "Given a StreamingQuery, this step will invoke the monitor thread and wait while records are processed. The monitor class will be used to stop the query and determine if further processing should occur.",
    "branch", "Streaming")
  @BranchResults(List("continue", "stop"))
  @StepParameters(Map("query" -> StepParameter(None, Some(false), None, None, None, None, Some("The streaming query to monitor")),
    "streamingMonitorClassName" -> StepParameter(None, Some(false), None, None, None, None, Some("Fully qualified classname of the monitor class"))))
  @StepResults(primaryType = "String", secondaryTypes = Some(Map("$globals.$*" -> "Any")))
  def monitorStreamingQuery(query: Option[StreamingQuery],
                            streamingMonitorClassName: Option[String] = Some("com.acxiom.pipeline.streaming.BaseStreamingQueryMonitor"),
                            pipelineContext: PipelineContext): PipelineStepResponse = {
    if (query.isDefined) {
      logger.info("StreamingQuery defined, preparing monitor")
      val iteration = pipelineContext.getGlobalAs[Int]("STREAMING_MONITOR_ITERATION")
      val ctx = pipelineContext
        .setGlobal("STREAMING_MONITOR_START_DATE", new Date())
        .setGlobal("STREAMING_MONITOR_ITERATION", iteration.getOrElse(0) + 1)
      val streamingClassName = streamingMonitorClassName.getOrElse("com.acxiom.pipeline.streaming.BaseStreamingQueryMonitor")
      logger.info(s"Using monitor class: $streamingClassName")
      val queryMonitor = ReflectionUtils.loadClass(streamingClassName,
        Some(Map("query" -> query, "pipelineContext" -> ctx)))
        .asInstanceOf[StreamingQueryMonitor]
      queryMonitor.start()
      val awaitTimeOut = pipelineContext.getGlobalAs[String]("STREAMING_QUERY_TIMEOUT_MS")
      if (awaitTimeOut.getOrElse("NOT_A_NUMBER").forall(_.isDigit)) {
        query.get.awaitTermination(awaitTimeOut.get.toLong)
      } else {
        query.get.awaitTermination()
      }
      val state = if (queryMonitor.continue) { "continue" } else { "stop" }
      logger.info(s"SteamingQuery stopped with state $state")
      PipelineStepResponse(Some(state),
        Some(queryMonitor.getGlobalUpdates.foldLeft(Map[String, Any]())((globals, entry) => {
        globals + (s"$$globals.${entry._1}" -> entry._2)
      })))
    } else {
      logger.info("StreamingQuery not defined, setting state stop")
      PipelineStepResponse(Some("stop"), None)
    }
  }
}
