package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.PipelineExecutor
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger

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
    if (driverSetup.pipeline.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid pipeline. Please check the DriverSetup class: ${commonParameters.initializationClass}")
    }
    // TODO Determine how Spark streaming will work going forward.
    try {
      PipelineExecutor.executePipelines(driverSetup.pipeline.get, driverSetup.pipelineContext)
      // TODO PipelineListener should be used to indicate when the application has stopped/completed so additional cleanup can be performed
    } catch {
      case t: Throwable =>
        logger.error(s"Error while attempting to run application!", t)
        throw t
    }
  }
}
