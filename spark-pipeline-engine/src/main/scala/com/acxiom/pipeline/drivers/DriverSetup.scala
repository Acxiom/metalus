package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineExecution}

trait DriverSetup {
  val parameters: Map[String, Any]

  /**
    * Returns the list of pipelines to execute as part of this application.
    * @return
    */
  @deprecated("use executionPlan", "1.1.0")
  def pipelines: List[Pipeline]
  @deprecated("use executionPlan", "1.1.0")
  def initialPipelineId: String
  @deprecated("use executionPlan", "1.1.0")
  def pipelineContext: PipelineContext
  @deprecated("use executionPlan", "1.1.0")
  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }

  /**
    * This function will return the execution plan to be used for the driver.
    * @since 1.1.0
    * @return An execution plan or None if not implemented
    */
  def executionPlan: Option[List[PipelineExecution]] = {
    // TODO Return a default execution plan using the "pipelines"
    None
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
}
