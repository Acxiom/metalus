package com.acxiom.pipeline.drivers

import com.acxiom.pipeline.{Pipeline, PipelineContext}

trait DriverSetup {
  val parameters: Map[String, Any]

  def pipelines: List[Pipeline]
  def initialPipelineId: String
  def pipelineContext: PipelineContext
  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }
}
