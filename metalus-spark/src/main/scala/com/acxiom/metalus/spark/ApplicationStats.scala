package com.acxiom.metalus.spark

import com.acxiom.metalus.{ForkData, PipelineStateKey}
import org.apache.log4j.Logger
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, StageInfo}

import scala.collection.mutable

case class ApplicationStats(jobs: mutable.Map[Int, JobDetails]) {
  private val logger = Logger.getLogger(getClass)

  def startJob(jobStart: SparkListenerJobStart, executionInfo: PipelineStateKey): Unit = {
    val stageInfoMap = mutable.Map[Int, StageInfo]()
    jobStart.stageInfos.foreach(i => stageInfoMap(i.stageId) = i)
    this.jobs(jobStart.jobId) = JobDetails(
      jobStart.jobId, jobStart.time, None, None, Some(executionInfo.pipelineId),
      executionInfo.stepId, Some(executionInfo.forkData.getOrElse(ForkData(-1, None, None)).generateForkKeyValue()), stageInfoMap
    )
  }

  def endJob(jobEnd: SparkListenerJobEnd): Unit = {
    val currentJob = this.jobs.get(jobEnd.jobId)
    if (currentJob.isDefined) {
      this.jobs(jobEnd.jobId) = currentJob.get.copy(end = Some(jobEnd.time), status = Some(jobEnd.jobResult.toString))
    } else {
      logger.warn(s"jobEnd signal received with no tracked jobs")
    }
  }

  def endStage(stageEnd: SparkListenerStageCompleted): Unit = {
    this.jobs.foreach(j => {
      j._2.stages.foreach(s => {
        if (s._1 == stageEnd.stageInfo.stageId) {
          j._2.stages(stageEnd.stageInfo.stageId) = stageEnd.stageInfo
        }
      })
    })
  }

  def reset(): Unit = {
    jobs.clear()
  }

  def isActive: Boolean = jobs.nonEmpty

  def getSummary(pipelineId: Option[String] = None, stepId: Option[String] = None, groupId: Option[String] = None): List[Map[String, Any]] = {
    val results = this.jobs.filter(x => {
      x._2.pipelineId.isDefined && x._2.stepId.isDefined && pipelineId.isDefined && stepId.isDefined &&
        x._2.pipelineId.get == pipelineId.get && x._2.stepId.get == stepId.get && x._2.forkData.getOrElse("") == groupId.getOrElse("")
    })
      .map(j => {
        val stageStats = j._2.stages.map(s => {
          this.stageStatsToMap(s._2)
        })

        val duration = if (j._2.end.isDefined) {
          j._2.end.get - j._2.start
        } else {
          -1
        }

        Map(
          "jobId" -> j._1, "pipelineId" -> j._2.pipelineId.getOrElse(""), "stepId" -> j._2.stepId.getOrElse(""),
          "groupId" -> j._2.forkData.getOrElse(""), "status" -> j._2.status.getOrElse(""), "start" -> j._2.start,
          "end" -> j._2.end.getOrElse(""), "durationMs" -> duration, "stages" -> stageStats.toList
        )
      }).toList
    results
  }

  private def stageStatsToMap(stage: StageInfo): Map[String, Any] = {
    val duration = if (stage.completionTime.isDefined && stage.submissionTime.isDefined) {
      stage.completionTime.get - stage.submissionTime.get
    }

    val basicStats = Map(
      "stageId" -> stage.stageId, "stageName" -> stage.name, "attemptNumber" -> stage.attemptNumber,
      "startTime" -> stage.submissionTime, "endTime" -> stage.completionTime, "failureReason" -> stage.failureReason,
      "tasks" -> stage.numTasks, "parentIds" -> stage.parentIds, "start" -> stage.submissionTime,
      "end" -> stage.completionTime, "durationMs" -> duration
    )

    // add task stats if available
    if (stage.taskMetrics == null) {
      basicStats
    } else {
      val task = stage.taskMetrics

      val inMetrics = if (task.inputMetrics != null) {
        Map("bytesRead" -> task.inputMetrics.bytesRead, "recordsRead" -> task.inputMetrics.recordsRead)
      } else {
        Map()
      }

      val outMetrics = if (task.outputMetrics != null) {
        val out = task.outputMetrics
        Map(
          "bytesWritten" -> out.bytesWritten, "recordsWritten" -> out.recordsWritten
        )
      } else {
        Map()
      }

      Map(
        "cpuTime" -> task.executorCpuTime, "gcTime" -> task.jvmGCTime,
        "executorRunTime" -> task.executorRunTime, "executorCpuTime" -> task.executorCpuTime,
        "peakExecutorMemory" -> task.peakExecutionMemory
      ) ++ inMetrics ++ outMetrics ++ basicStats
    }
  }
}

case class JobDetails(jobId: Int, start: Long, end: Option[Long], status: Option[String], pipelineId: Option[String],
                      stepId: Option[String], forkData: Option[String], stages: mutable.Map[Int, StageInfo])


