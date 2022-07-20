package com.acxiom.pipeline.drivers

import com.acxiom.pipeline._
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.log4j.{Level, Logger}

trait DriverSetup {
  val parameters: Map[String, Any]

  private val logger = Logger.getLogger(getClass)
  setLogLevel()

  private final val deprecationSuggestion = "use executionPlan"

  private lazy val provider = DriverUtils.getCredentialProvider(parameters)

  /**
    * Returns the list of pipelines to execute as part of this application.
    *
    * @return
    */
  @deprecated(deprecationSuggestion, "1.1.0")
  def pipelines: List[Pipeline] = List()
  @deprecated(deprecationSuggestion, "1.1.0")
  def initialPipelineId: String = ""
  @deprecated(deprecationSuggestion, "1.1.0")
  def pipelineContext: PipelineContext
  @deprecated(deprecationSuggestion, "1.1.0")
  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }

  /**
    * Returns the CredentialProvider to use during for this job.
    * @return The credential provider.
    */
  def credentialProvider: CredentialProvider = provider

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
    * @param results Optional results of a previous run
    * @since 1.1.0
    * @return An execution plan
    */
  def refreshExecutionPlan(executionPlan: List[PipelineExecution],
                           results: Option[Map[String, DependencyResult]] = None): List[PipelineExecution] = {
    executionPlan
  }

  /**
    * Once the execution completes, parse the results. If an unexpected exception was thrown, it will be thrown. The
    * result of this function will be false if the success is false and the process was not paused. A pause will
    * result in this function returning true.
    * @param results The execution results
    * @return true if everything executed properly (or paused), false if anything failed. Any non-pipeline errors will be thrown
    */
  def handleExecutionResult(results: Option[Map[String, DependencyResult]]): ResultSummary =
    DriverUtils.handleExecutionResult(results)

  def setLogLevel(): Unit = {
    logger.info("Setting logging level")
    Logger.getRootLogger.setLevel(getLogLevel(parameters.getOrElse("rootLogLevel", "WARN").asInstanceOf[String]))
    Logger.getLogger("com.acxiom").setLevel(getLogLevel(parameters.getOrElse("logLevel", "INFO").asInstanceOf[String]))
    if (parameters.contains("customLogLevels") && parameters("customLogLevels").toString.trim.nonEmpty) {
      val customLogLevels = parameters("customLogLevels").asInstanceOf[String]
      customLogLevels.split(",").foreach(level => {
        val logParams = level.split(":")
        Logger.getLogger(logParams.head).setLevel(getLogLevel(logParams(1)))
      })
    }
  }

  private def getLogLevel(level: String): Level = {
    DriverUtils.getLogLevel(level)
  }
}

case class ResultSummary(success: Boolean, failedExecution: Option[String], failedPipeline: Option[String])
