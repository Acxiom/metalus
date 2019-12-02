package com.acxiom.pipeline.metrics

import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineListener, PipelineStep}
import org.apache.log4j.Logger
import org.apache.spark.scheduler._

class MetricsPipelineListener(metricsLevel: Int = 0) extends SparkListener with PipelineListener {
  private val logger = Logger.getLogger(getClass)

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(pipelineContext)
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(pipelineContext)
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(pipelineContext)
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    Some(pipelineContext)
  }

  /*
   * Application
   *  Executor
   *    Job
   *      Stage
   *        Task
   *
   * block updated happens at stage and task level
   */

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = printEvent(applicationStart)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = printEvent(applicationEnd)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = printEvent(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = printEvent(jobEnd)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = printEvent(stageSubmitted)

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = printEvent(stageCompleted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = printEvent(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = printEvent(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = printEvent(taskEnd)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = printEvent(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = printEvent(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = printEvent(executorRemoved)



//  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = printEvent(environmentUpdate)
//
//  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = printEvent(blockManagerAdded)
//
//  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = printEvent(blockManagerRemoved)
//
//  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = printEvent(unpersistRDD)
//
//  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = printEvent(executorBlacklisted)
//
//  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = printEvent(executorUnblacklisted)
//
//  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = printEvent(nodeBlacklisted)
//
//  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = printEvent(nodeUnblacklisted)
//
//  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = printEvent(blockUpdated)
//
//  override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = printEvent(speculativeTask)

//  override def onOtherEvent(event: SparkListenerEvent): Unit ={
//    logger.info(s"Other event fired: $event")
//  }

  private def printEvent(event: SparkListenerEvent): Unit = {
    logger.info(s"Audit Spark Event: $event")
  }
}
