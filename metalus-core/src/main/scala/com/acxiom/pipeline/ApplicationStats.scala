package com.acxiom.pipeline

import org.apache.spark.scheduler._
import org.apache.log4j.Logger

import scala.collection.mutable

case class ApplicationStats(jobs: mutable.Map[Int, JobDetails]) {
  private val logger = Logger.getLogger(getClass)

  def startJob(jobStart: SparkListenerJobStart, executionInfo: PipelineExecutionInfo): Unit = {
    val stageInfoMap = mutable.Map[Int, StageInfo]()
    jobStart.stageInfos.foreach(i => stageInfoMap(i.stageId) = i)
    this.jobs(jobStart.jobId) = JobDetails(
      jobStart.jobId, jobStart.time, None, None, executionInfo.pipelineId, executionInfo.stepId, executionInfo.groupId, stageInfoMap
    )
  }

  def endJob(jobEnd: SparkListenerJobEnd): Unit = {
    val currentJob = this.jobs.get(jobEnd.jobId)
    if(currentJob.isDefined) {
      this.jobs(jobEnd.jobId) = currentJob.get.copy(end = Some(jobEnd.time), status = Some(jobEnd.jobResult.toString))
    } else {
      logger.warn(s"jobEnd signal received with no tracked jobs")
    }
  }

  def endStage(stageEnd: SparkListenerStageCompleted): Unit = {
    this.jobs.foreach(j => {
      j._2.stages.foreach(s => {
        if(s._1 == stageEnd.stageInfo.stageId) {
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
        x._2.pipelineId.get == pipelineId.get && x._2.stepId.get == stepId.get && x._2.groupId.getOrElse("") == groupId.getOrElse("")
    })
      .map(j => {
        val stageStats = j._2.stages.map(s => {
          this.stageStatsToMap(s._2)
        })

        val duration = if(j._2.end.isDefined) {
          j._2.end.get - j._2.start
        } else {
          ""
        }

        Map(
          "jobId" -> j._1, "pipelineId" -> j._2.pipelineId, "stepId" -> j._2.stepId, "groupId" -> j._2.groupId, "status" -> j._2.status,
          "start" -> j._2.start, "end" -> j._2.end, "durationMs" -> duration, "stages" -> stageStats.toList
        )
      }).toList
    results
  }

  private def stageStatsToMap(stage: StageInfo): Map[String, Any] = {
    val task = stage.taskMetrics
    val in = task.inputMetrics
    val out= task.outputMetrics
    val clockTime = if (stage.completionTime.isDefined && stage.submissionTime.isDefined) {
      stage.completionTime.get - stage.submissionTime.get
    } else { -1 }
    val duration = if(stage.submissionTime.isDefined && stage.completionTime.isDefined) {
      stage.completionTime.get - stage.submissionTime.get
    } else { "" }
    Map(
      "stageId" -> stage.stageId, "stageName" -> stage.name, "attemptId" -> stage.attemptId,
      "startTime" -> stage.submissionTime, "endTime" -> stage.completionTime, "clockTime" -> clockTime,
      "bytesRead" -> in.bytesRead, "recordsRead" -> in.recordsRead,
      "bytesWritten" -> out.bytesWritten, "recordsWritten" -> out.recordsWritten,
      "cpuTime" -> task.executorCpuTime, "gcTime" -> task.jvmGCTime,
      "executorRunTime" -> task.executorRunTime, "executorCpuTime" -> task.executorCpuTime,
      "peakExecutorMemory" -> task.peakExecutionMemory, "failureReason" -> stage.failureReason,
      "tasks" -> stage.numTasks, "parentIds" -> stage.parentIds, "start" -> stage.submissionTime,
      "end" -> stage.completionTime, "durationMs" -> duration
    )
  }
}

case class ExecutorDetails(executorId: String, active: Boolean, start: Long, host: String, totalCores: Int,
                           end: Option[Long], removedReason: Option[String], updates: Option[Map[Long, Any]])

case class JobDetails(jobId: Int, start: Long, end: Option[Long], status: Option[String], pipelineId: Option[String],
                      stepId: Option[String], groupId: Option[String], stages: mutable.Map[Int, StageInfo])


