package com.acxiom.metalus.drivers

import com.acxiom.metalus.PipelineExecutor
import com.acxiom.metalus.utils.{DriverUtils, ReflectionUtils}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Provides a basic driver that will read in command line parameters and execute pipelines. The only required parameter
  * is "--driverSetupClass" which is the fully qualified class name of the "DriverSetup" implementation. This class will
  * handle all of the initial setup such as building out pipelines, verifying required parameters are present and
  * creating the PipelineContext.
  */
object DefaultPipelineDriver {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass")))
    val commonParameters = DriverUtils.parseCommonParameters(parameters)
    val driverSetup = ReflectionUtils.loadClass(commonParameters.initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.pipeline.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid pipeline. Please check the DriverSetup class: ${commonParameters.initializationClass}")
    }
    try {
      PipelineExecutor.executePipelines(driverSetup.pipeline.get, driverSetup.pipelineContext)
    } catch {
      case t: Throwable =>
        logger.error(s"Error while attempting to run application!", t)
        throw t
    }
  }
}
