package com.acxiom.pipeline

import scala.collection.mutable.ListBuffer
import org.apache.spark.scheduler._
import org.apache.log4j.Logger

case class ApplicationStats(jobs: ListBuffer[JobDetails]) {
  private val logger = Logger.getLogger(getClass)
  private var currentJob:Option[JobDetails] = None

  def startJob(jobStart: SparkListenerJobStart, executionInfo: PipelineExecutionInfo): Unit = {
    currentJob = Some(JobDetails(jobStart.jobId, jobStart.time, None, None, executionInfo.pipelineId, executionInfo.stepId, ListBuffer()))
  }

  def endJob(jobEnd: SparkListenerJobEnd): Unit = {
    if(currentJob.isDefined) {
      this.jobs += currentJob.get.copy(end = Some(jobEnd.time), status = Some(jobEnd.jobResult.toString))
    } else {
      logger.warn(s"jobEnd signal received but current job stat was empty")
    }
    currentJob = None
  }

  def endStage(stageEnd: SparkListenerStageCompleted): Unit = {
    currentJob.get.stages += stageEnd.stageInfo
  }

  def reset(): Unit = {
    jobs.clear()
  }

  def getSummary(): List[Map[String, Any]] = {
    this.jobs.map(j => {
      val stageStats = j.stages.map(s => {
        this.stageStatsToMap(s)
      })

      Map("jobId" -> j.jobId, "pipelineId" -> j.pipelineId, "stepId" -> j.stepId, "status" -> j.status,
        "startTime" -> j.start, "endTime" -> j.end, "stages" -> stageStats.toList)
    }).toList
  }

  private def stageStatsToMap(stage: StageInfo): Map[String, Any] = {
    val task = stage.taskMetrics
    val in = task.inputMetrics
    val out= task.outputMetrics
    val clockTime = if (stage.completionTime.isDefined && stage.submissionTime.isDefined) {
      stage.completionTime.get - stage.submissionTime.get
    } else { -1 }
    Map(
      "stageId" -> stage.stageId, "stageName" -> stage.name, "attemptId" -> stage.attemptId,
      "startTime" -> stage.submissionTime, "endTime" -> stage.completionTime, "clockTime" -> clockTime,
      "bytesRead" -> in.bytesRead, "recordsRead" -> in.recordsRead,
      "bytesWritten" -> out.bytesWritten, "recordsWritten" -> out.recordsWritten,
      "cpuTime" -> task.executorCpuTime, "gcTime" -> task.jvmGCTime,
      "executorRunTime" -> task.executorRunTime, "executorCpuTime" -> task.executorCpuTime,
      "peakExecutorMemory" -> task.peakExecutionMemory, "failureReason" -> stage.failureReason,
      "tasks" -> stage.numTasks, "parentIds" -> stage.parentIds
    )
  }
}

case class ExecutorDetails(executorId: String, active: Boolean, start: Long, host: String, totalCores: Int,
                           end: Option[Long], removedReason: Option[String], updates: Option[Map[Long, Any]])

case class JobDetails(jobId: Int, start: Long, end: Option[Long], status: Option[String], pipelineId: Option[String],
                      stepId: Option[String], stages: ListBuffer[StageInfo])


