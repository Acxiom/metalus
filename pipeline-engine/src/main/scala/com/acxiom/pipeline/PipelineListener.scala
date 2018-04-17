package com.acxiom.pipeline

import org.apache.log4j.Logger

object PipelineListener {
  def apply(): PipelineListener = DefaultPipelineListener()
}

case class DefaultPipelineListener() extends PipelineListener {}

trait PipelineListener {
  private val logger = Logger.getLogger(getClass)

  def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    logger.info(s"Starting execution of pipelines ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse("")))}")
  }

  def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    logger.info(s"Starting execution of pipelines ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse("")))}")
  }

  def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    logger.info(s"Stopping execution of pipelines. Completed: ${pipelines.map(p => p.name.getOrElse(p.id.getOrElse("")))}")
  }

  def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Unit = {
    logger.info(s"Starting pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
  }

  def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Unit = {
    logger.info(s"Finished pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
  }

  def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Unit = {
    logger.info(s"Starting step ${step.displayName.getOrElse(step.id.getOrElse(""))} of pipeline ${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
  }

  def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Unit = {
    logger.info(s"Finished step ${step.displayName.getOrElse(step.id.getOrElse(""))} of pipeline${pipeline.name.getOrElse(pipeline.id.getOrElse(""))}")
  }

  def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {}
}
