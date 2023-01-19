package com.acxiom.metalus.drivers

import com.acxiom.metalus._
import com.acxiom.metalus.utils.{DriverUtils, ReflectionUtils}
import org.slf4j.{Logger, LoggerFactory}

trait DriverSetup {
  val parameters: Map[String, Any]

  private val logger = LoggerFactory.getLogger(getClass)
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
  private def setLogLevel(): Unit = {
    logger.info("Setting logging level")
    setLevel(Logger.ROOT_LOGGER_NAME, parameters.getOrElse("rootLogLevel", "WARN").asInstanceOf[String])
    setLevel("com.acxiom", parameters.getOrElse("logLevel", "WARN").asInstanceOf[String])
    if (parameters.contains("customLogLevels") && parameters("customLogLevels").toString.trim.nonEmpty) {
      val customLogLevels = parameters("customLogLevels").asInstanceOf[String]
      customLogLevels.split(",").foreach(level => {
        val logParams = level.split(":")
        setLevel(logParams.head, logParams(1))
      })
    }
  }

  /**
   * This function will attempt to determine the logger implementation and set the level.
   * @param logName The logger path
   * @param level Thee level to set
   */
  private def setLevel(logName: String, level: String): Unit = {
    val log = LoggerFactory.getLogger(logName)
    log.getClass.getName match {
      // This will handle log4j 1.x and reload4j
      case "org.apache.log4j.Logger" =>
        ReflectionUtils.executeFunctionByName(log, "setLevel",
          List(Class.forName("org.apache.log4j.Level")
            .getDeclaredField(Option(level).getOrElse("INFO").toUpperCase).get(None.orNull)))
      case "java.util.logging.Logger" =>
        ReflectionUtils.executeFunctionByName(log, "setLevel",
          List(Class.forName("java.util.logging.Level")
            .getDeclaredField(convertLevelToJUL(level)).get(None.orNull)))
      case "ch.qos.logback.classic.Logger" =>
        ReflectionUtils.executeFunctionByName(log, "setLevel",
          List(Class.forName("ch.qos.logback.classic.Level")
            .getDeclaredField(Option(level).getOrElse("INFO").toUpperCase).get(None.orNull)))
      case _ =>
    }
  }

  /**
   * Converts slf4j, log4j and logback standard levels to java util Logging levels
   * @param level The level to convert.
   * @return A level string that can be used for java logging.
   */
  private def convertLevelToJUL(level: String): String = {
    Option(level).getOrElse("").toUpperCase match {
      case "ERROR" => "SEVERE"
      case "WARN" => "WARNING"
      case "DEBUG" => "FINE"
      case "TRACE" => "FINEST"
      case _ => level
    }
  }
}
