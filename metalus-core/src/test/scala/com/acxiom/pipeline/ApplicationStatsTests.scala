package com.acxiom.pipeline

//import com.acxiom.pipeline.utils.ReflectionUtils
//import org.apache.spark.scheduler._
//import org.joda.time.DateTime
//
//import scala.collection.mutable

import org.scalatest.funspec.AnyFunSpec

class ApplicationStatsTests extends AnyFunSpec {
//  describe("ApplicationStats - Basic Tests") {
//    it("Should create and manage ApplicationStats object") {
//      val stats = ApplicationStats(mutable.Map())
//      assert(stats.jobs.isEmpty)
//      assert(!stats.isActive)
//
//      // shared execInfo
//      val currTime = new DateTime()
//      val execInfo = PipelineExecutionInfo(Some("step-1"), Some("pipeline-1"), None, Some("group-1"))
//
//      // create stages for job 0
//      val initStagesJob0 = (0 to 4).map(x => {
//        createStageInfo(x, 1, "init-details")
//      })
//      val jobStart = SparkListenerJobStart(0, currTime.getMillis, initStagesJob0)
//
//      stats.startJob(jobStart, execInfo)
//      // make sure job is added
//      assert(stats.jobs.size == 1)
//      assert(stats.isActive)
//      assert(stats.jobs(0).stages.size == 5)
//      stats.jobs(0).stages.foreach(s => {
//        assert(s._2.details == "init-details")
//      })
//
//      // create stages for job 1
//      val initStagesJob1 = (5 to 7).map(x => {
//        createStageInfo(x, 2, "init-details")
//      })
//
//      stats.startJob(jobStart.copy(jobId = 1, stageInfos = initStagesJob1), execInfo.copy(stepId=Some("step-2")))
//      // make sure job is added
//      assert(stats.jobs.size == 2)
//      assert(stats.jobs(1).stages.size == 3)
//
//      // create stages for job 2 (for step-1)
//      val initStagesJob2 = (8 to 9).map(x => {
//        createStageInfo(x, 2, "init-details")
//      })
//
//      stats.startJob(jobStart.copy(jobId = 2, stageInfos = initStagesJob2), execInfo.copy(stepId=Some("step-2")))
//      // make sure job is added
//      assert(stats.jobs.size == 3)
//      assert(stats.jobs(2).stages.size == 2)
//
//      // update the stages
//      (0 to 6).foreach(x => {
//        val updatedStageInfo = createStageInfo(x, 1, "updated-details")
//        stats.endStage(SparkListenerStageCompleted(updatedStageInfo))
//      })
//
//      val endTime = currTime.getMillis + 1000L
//      // end the job
//      stats.endJob(SparkListenerJobEnd(0, endTime, org.apache.spark.scheduler.JobSucceeded))
//      stats.endJob(SparkListenerJobEnd(1, endTime + 1000L, org.apache.spark.scheduler.JobSucceeded))
//      stats.endJob(SparkListenerJobEnd(2, endTime + 2000L, org.apache.spark.scheduler.JobSucceeded))
//
//      val job0 = stats.jobs(0)
//      assert(job0.start == currTime.getMillis)
//      assert(job0.pipelineId == execInfo.pipelineId)
//      assert(job0.stepId == execInfo.stepId)
//      assert(job0.groupId == execInfo.groupId)
//      assert(job0.stages.size == 5)
//      assert(job0.end.contains(endTime))
//      assert(job0.status.nonEmpty)
//      (0 to 4).foreach(x => {
//        assert(job0.stages.contains(x))
//        assert(job0.stages(x).details == "updated-details")
//      })
//
//      // verify the summary for step 1
//      val step1 = stats.getSummary(Some("pipeline-1"), Some("step-1"), Some("group-1"))
//      assert(step1.exists(s => {
//        s.getOrElse("jobId", -1).asInstanceOf[Int] == 0
//        s.getOrElse("stepId", "").asInstanceOf[String] == "step-1"
//        s.getOrElse("groupId", "").asInstanceOf[String] == "group-1"
//        s.getOrElse("durationMs", 0L).asInstanceOf[Long] == 1000L
//        s.getOrElse("stages", List()).asInstanceOf[List[Any]].length == 5
//      }))
//
//      val job1 = stats.jobs(1)
//      assert(job1.start == currTime.getMillis)
//      assert(job1.pipelineId == execInfo.pipelineId)
//      assert(job1.stepId.contains("step-2"))
//      assert(job1.groupId == execInfo.groupId)
//      assert(job1.end.contains(endTime + 1000L))
//      assert(job1.stages.size == 3)
//      assert(job1.status.nonEmpty)
//
//      (5 to 7).foreach(x => {
//        assert(job1.stages.contains(x))
//        if (x == 7) {
//          assert(job1.stages(x).details == "init-details")
//        } else {
//          assert(job1.stages(x).details == "updated-details")
//        }
//      })
//
//      val job2 = stats.jobs(2)
//      assert(job2.start == currTime.getMillis)
//      assert(job2.pipelineId == execInfo.pipelineId)
//      assert(job2.stepId.contains("step-2"))
//      assert(job2.groupId == execInfo.groupId)
//      assert(job2.end.contains(endTime + 2000L))
//      assert(job2.stages.size == 2)
//      assert(job2.status.nonEmpty)
//
//      (8 to 9).foreach(x => {
//        assert(job2.stages.contains(x))
//      })
//
//      // verify summary for step-2
//      val step2 = stats.getSummary(Some("pipeline-1"), Some("step-2"), Some("group-1"))
//      assert(step2.exists(s => {
//        s.getOrElse("jobId", -1).asInstanceOf[Int] == 1
//        s.getOrElse("stepId", "").asInstanceOf[String] == "step-2"
//        s.getOrElse("groupId", "").asInstanceOf[String] == "group-1"
//        s.getOrElse("durationMs", 0L).asInstanceOf[Long] == 1000L
//        s.getOrElse("stages", List()).asInstanceOf[List[Any]].length == 3
//      }))
//
//      assert(step2.exists(s => {
//        s.getOrElse("jobId", -1).asInstanceOf[Int] == 2
//        s.getOrElse("stepId", "").asInstanceOf[String] == "step-2"
//        s.getOrElse("groupId", "").asInstanceOf[String] == "group-1"
//        s.getOrElse("durationMs", 0L).asInstanceOf[Long] == 2000L
//        s.getOrElse("stages", List()).asInstanceOf[List[Any]].length == 2
//      }))
//
//      stats.reset()
//      assert(!stats.isActive)
//    }
//  }
//
//  def createStageInfo(stageId: Int, attemptId: Int, details: String): StageInfo = {
//    ReflectionUtils.loadClass("org.apache.spark.scheduler.StageInfo", Some(Map[String, Any](
//      "stageId" -> stageId,
//      "attemptId" -> attemptId,
//      "name" -> s"test-stage-$stageId",
//      "numTasks" -> 1,
//      "rddInfos" -> Seq.empty,
//      "parentIds" -> Seq.empty,
//      "details" -> details,
//      "resourceProfileId" -> 0
//    ))).asInstanceOf[StageInfo]
//  }
}
