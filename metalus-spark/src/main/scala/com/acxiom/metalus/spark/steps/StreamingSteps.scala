package com.acxiom.metalus.spark.steps

import com.acxiom.metalus.annotations._
import com.acxiom.metalus.drivers.DefaultPipelineDriver.logger
import com.acxiom.metalus.spark.streaming.StreamingQueryMonitor
import com.acxiom.metalus.utils.ReflectionUtils
import com.acxiom.metalus.{PipelineContext, PipelineStepResponse}
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Date

@StepObject
object StreamingSteps {
  @StepFunction("64c983e2-5eac-4fb6-87b2-024b69aa0ded",
    "Streaming Monitor",
    "Given a StreamingQuery, this step will invoke the monitor thread and wait while records are processed. The monitor class will be used to stop the query and determine if further processing should occur.",
    "branch", "Streaming", List[String]("streaming"))
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
      val state = if (queryMonitor.continue) {
        "continue"
      } else {
        "stop"
      }
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
