package com.acxiom.metalus.spark

import com.acxiom.metalus.{PipelineContext, PipelineListener, PipelineStateKey}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable

trait SparkPipelineListener extends SparkListener with PipelineListener {
  private var currentExecutionInfo: Option[PipelineStateKey] = None
  private val applicationStats: ApplicationStats = ApplicationStats(mutable.Map())
  private val sparkSettingToReport: List[String] = List(
    "spark.kryoserializer.buffer.max", "spark.driver.memory", "spark.executor.memory", "spark.sql.shuffle.partitions",
    "spark.driver.cores", "spark.executor.cores", "spark.default.parallelism", "spark.driver.memoryOverhead",
    "spark.executor.memoryOverhead"
  )

// TODO Figure out how this should work moving forward
//  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
//    val defaultContext = pipelineContext.setRootAuditMetric(Constants.SPARK_SETTINGS, this.getSparkSettingsForAudit(pipelineContext))
//    val finalContext = if (pipelineContext.getGlobalAs[Boolean]("includeAllSparkSettingsInAudit").contains(true)) {
//      val sparkContext = pipelineContext.contextManager.getContext("spark").asInstanceOf[Option[SparkSessionContext]]
//      if (sparkContext.isDefined) {
//        defaultContext.setRootAuditMetric("test-sparkContext", sparkContext.get.sparkSession.sparkContext.getConf.getAll)
//      } else {
//        defaultContext
//      }
//    } else { defaultContext }
//
//    applicationStats.reset()
//    Some(finalContext)
//  }

  // TODO [2.0 Review] Fix once core is ready
//  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
//    this.currentExecutionInfo = Some(pipelineContext.currentStateInfo.get)
//    None
//  }

  // TODO [2.0 Review] Fix once core is ready
//  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext):  Option[PipelineContext] = {
//    super.pipelineFinished(pipeline, pipelineContext)
//    None
//  }

  // TODO [2.0 Review] Fix once core is ready
//  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
//    this.currentExecutionInfo = Some(pipelineContext.currentStateInfo.get)
//    super.pipelineStepStarted(pipeline, step, pipelineContext)
//    None
//  }

  // TODO [2.0 Review] Fix once core is ready
//  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
//    val newContext = if (pipeline.id.isDefined && step.id.isDefined) {
//      pipelineContext.setStepMetric(
//        pipeline.id.get, step.id.get, groupId, Constants.SPARK_METRICS, this.applicationStats.getSummary(pipeline.id, step.id, groupId)
//      )
//    } else {
//      pipelineContext
//    }
//    super.pipelineStepFinished(pipeline, step, newContext)
//    Some(newContext)
//  }

  override def onJobStart(jobStart: SparkListenerJobStart): scala.Unit = {
    if (this.currentExecutionInfo.isDefined) {
      this.applicationStats.startJob(jobStart, this.currentExecutionInfo.get)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): scala.Unit = {
    this.applicationStats.endJob(jobEnd)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.applicationStats.endStage(stageCompleted)
  }

  def getSparkSettingsForAudit(pipelineContext: PipelineContext): Map[String, Any] = {
    pipelineContext.sparkSessionOption.map{ spark =>
      this.sparkSettingToReport.map(s => s -> spark.sparkContext.getConf.getOption(s)).toMap
    }.getOrElse(Map())
  }
}
