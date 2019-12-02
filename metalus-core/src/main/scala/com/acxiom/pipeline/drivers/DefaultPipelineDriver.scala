package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.PipelineDependencyExecutor
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}

/**
  * Provides a basic driver that will read in command line parameters and execute pipelines. The only required parameter
  * is "--driverSetupClass" which is the fully qualified class name of the "DriverSetup" implementation. This class will
  * handle all of the initial setup such as building out pipelines, identifying the initialPipelineId is present and
  * creating the PipelineContext.
  */
object DefaultPipelineDriver {
  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("driverSetupClass")))
    val initializationClass = parameters("driverSetupClass").asInstanceOf[String]
    val driverSetup = ReflectionUtils.loadClass(initializationClass,
      Some(Map("parameters" -> parameters))).asInstanceOf[DriverSetup]
    if (driverSetup.executionPlan.isEmpty) {
      throw new IllegalStateException(s"Unable to obtain valid execution plan. Please check the DriverSetup class: $initializationClass")
    }
    PipelineDependencyExecutor.executePlan(driverSetup.executionPlan.get)
  }
}
