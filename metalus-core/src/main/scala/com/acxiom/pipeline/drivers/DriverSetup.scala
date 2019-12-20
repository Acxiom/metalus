package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineExecution}
import org.apache.log4j.{Level, Logger}

trait DriverSetup {
  val parameters: Map[String, Any]

  private val logger = Logger.getLogger(getClass)
  setLogLevel()

  private val deprecationSuggestion = "use executionPlan"

  /**
    * Returns the list of pipelines to execute as part of this application.
    *
    * @return
    */
  @deprecated(deprecationSuggestion, "1.1.0")
  def pipelines: List[Pipeline]
  @deprecated(deprecationSuggestion, "1.1.0")
  def initialPipelineId: String
  @deprecated(deprecationSuggestion, "1.1.0")
  def pipelineContext: PipelineContext
  @deprecated(deprecationSuggestion, "1.1.0")
  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }

  /**
    * This function will return the execution plan to be used for the driver.
    * @since 1.1.0
    * @return An execution plan or None if not implemented
    */
  def executionPlan: Option[List[PipelineExecution]] = {
    if (Option(this.pipelines).isDefined && this.pipelines.nonEmpty) {
      logger.warn("Pipelines have been defined using deprecated pipelines function, creating default execution plan")
      val initialId = if (Option(this.initialPipelineId).isDefined && this.initialPipelineId.nonEmpty) Some(this.asInstanceOf) else None
      Some(List(PipelineExecution("0", this.pipelines, initialId, this.pipelineContext)))
    } else {
      logger.warn("No pipelines defined, returning no execution plan")
      None
    }
  }

  /**
    * This function allows the driver setup a chance to refresh the execution plan. This is useful in long running
    * applications such as streaming where artifacts build up over time.
    *
    * @param executionPlan The execution plan to refresh
    * @since 1.1.0
    * @return An execution plan
    */
  def refreshExecutionPlan(executionPlan: List[PipelineExecution]): List[PipelineExecution] = {
    executionPlan
  }

  def setLogLevel(): Unit = {
    logger.info("Setting logging level")
    Logger.getRootLogger.setLevel(getLogLevel(parameters.getOrElse("rootLogLevel", "WARN").asInstanceOf[String]))
    Logger.getLogger("com.acxiom").setLevel(getLogLevel(parameters.getOrElse("logLevel", "INFO").asInstanceOf[String]))
  }

  private def getLogLevel(level: String): Level = {
    Option(level).getOrElse("INFO").toUpperCase match {
      case "INFO" => Level.INFO
      case "DEBUG" => Level.DEBUG
      case "ERROR" => Level.ERROR
      case "WARN" => Level.WARN
      case "TRACE" => Level.TRACE
      case "FATAL" => Level.FATAL
      case "OFF" => Level.OFF
    }
  }
}
