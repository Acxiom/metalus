package com.acxiom.pipeline.drivers

import com.acxiom.pipeline._
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.log4j.{Level, Logger}

trait DriverSetup {
  val parameters: Map[String, Any]

  private val logger = Logger.getLogger(getClass)
  setLogLevel()

  private lazy val provider = DriverUtils.getCredentialProvider(parameters)

  /**
    * Returns the main pipeline being executed.
    * @return The main pipeline to execute.
    */
  def pipeline: Option[Pipeline]

  /**
    * The PipelineContext to use when executing the Pipeline
    * @return
    */
  def pipelineContext: PipelineContext

  // TODO [2.0 Follow Up] Is this still needed?
  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }

  /**
    * Returns the CredentialProvider to use during for this job.
    * @return The credential provider.
    */
  def credentialProvider: CredentialProvider = provider

  /**
    * Uses the input parameters to determine log levels
    */
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
