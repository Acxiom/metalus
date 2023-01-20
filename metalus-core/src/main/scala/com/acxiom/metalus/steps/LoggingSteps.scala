package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.metalus.utils.DriverUtils
import org.slf4j.{Logger, LoggerFactory}

@StepObject
object LoggingSteps {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  @StepFunction("931ad4e5-4501-4716-853a-30fbf8fb6090",
    "Log Message",
    "Log a simple message",
    "Pipeline", "Logging")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to log")),
    "level" -> StepParameter(None, Some(true), None, None, None, None, Some("Log level at which to log. Should be a valid log4j level"))))
  def logMessage(message: String, level: String): Unit = {
    logger.atLevel(DriverUtils.getLogLevel(level)).setMessage(message).log()
  }
}
