package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.utils.{CommonParameters, DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger

import scala.annotation.tailrec

/**
  * Provides a basic driver that will read in command line parameters and execute pipelines. The only required parameter
  * is "--driverSetupClass" which is the fully qualified class name of the "DriverSetup" implementation. This class will
  * handle all of the initial setup such as building out pipelines, identifying the initialPipelineId is present and
  * creating the PipelineContext.
  */
object DefaultPipelineDriver {
  val logger: Logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass")))
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    val driverSetup = ReflectionUtils.loadClass(commonParameters.initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid execution plan. Please check the DriverSetup class: ${commonParameters.initializationClass}")
    }
    try {
      process(driverSetup, commonParameters)
    } catch {
      case t: Throwable =>
        logger.error(s"Error while attempting to run application!", t)
        throw t
    }
  }

  @tailrec
  def process(driverSetup: DriverSetup, commonParameters: CommonParameters): Unit = {
    DriverUtils.processExecutionPlan(driverSetup, driverSetup.executionPlan.get, None, () => {},
      commonParameters.terminateAfterFailures, 1, commonParameters.maxRetryAttempts,
      commonParameters.streamingJob, commonParameters.rootExecutions)
    if (commonParameters.streamingJob) {
      process(driverSetup, commonParameters)
    }
  }
}
