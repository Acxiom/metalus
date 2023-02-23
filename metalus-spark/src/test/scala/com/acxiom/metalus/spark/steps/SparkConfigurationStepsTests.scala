package com.acxiom.metalus.spark.steps

import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.spark.SparkSessionContext
import com.acxiom.metalus.{ClassInfo, DefaultPipelineListener, PipelineContext, PipelineStepMapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.nio.file.{Files, Path}
import scala.language.postfixOps

class SparkConfigurationStepsTests extends AnyFunSpec with BeforeAndAfterAll with GivenWhenThen {
  private val MASTER = "local[2]"
  private val APPNAME = "spark-config-steps-spark"
  private var sparkSession: SparkSession = _
  private val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  private var pipelineContext: PipelineContext = _

  override def beforeAll(): Unit = {
    val contextManager = new ContextManager(Map("spark" ->
      ClassInfo(Some("com.acxiom.metalus.spark.SparkSessionContext"),
        Some(Map[String, Any]("sparkConfOptions" -> Map[String, Any](
          "setOptions" -> List(Map("name" -> "spark.local.dir", "value" -> sparkLocalDir.toFile.getAbsolutePath))),
          "appName" -> APPNAME,
          "sparkMaster" -> MASTER)))),
      Map())
    sparkSession = contextManager.getContext("spark").get.asInstanceOf[SparkSessionContext].sparkSession

    pipelineContext = PipelineContext(Some(Map[String, Any]()),
      List(), Some(List("com.acxiom.metalus.spark.steps")), PipelineStepMapper(),
      Some(DefaultPipelineListener()), contextManager = contextManager)
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("SparkConfigurationSteps - Basic") {
    it("should set a local property") {
      try {
        SparkConfigurationSteps.setLocalProperty("moo", "moo2", pipelineContext)
        assert(sparkSession.sparkContext.getLocalProperty("moo") == "moo2")
      } finally {
        sparkSession.sparkContext.setLocalProperty("moo", None.orNull)
      }
    }

    it("should unset a local property") {
      sparkSession.sparkContext.setLocalProperty("unset", "moo")
      SparkConfigurationSteps.setLocalProperty("unset", None, pipelineContext)
      assert(Option(sparkSession.sparkContext.getLocalProperty("unset")).isEmpty)
    }

    it ("should set a local properties") {
      try {
        SparkConfigurationSteps.setLocalProperties(Map("moo_m1" -> "m1", "moo_m2" -> "m2"), Some("_"), pipelineContext)
        assert(sparkSession.sparkContext.getLocalProperty("moo.m1") == "m1")
        assert(sparkSession.sparkContext.getLocalProperty("moo.m2") == "m2")
      } finally {
        sparkSession.sparkContext.setLocalProperty("moo.m1", None.orNull)
        sparkSession.sparkContext.setLocalProperty("moo.m2", None.orNull)
      }
    }

    it ("should unset a local properties") {
      try {
        sparkSession.sparkContext.setLocalProperty("moo.m1", "m1")
        sparkSession.sparkContext.setLocalProperty("moo.m2", "m2")
        SparkConfigurationSteps.setLocalProperties(Map("moo_m1" -> None, "moo_m2" -> None), Some("_"), pipelineContext)
        assert(Option(sparkSession.sparkContext.getLocalProperty("moo.m1")).isEmpty)
        assert(Option(sparkSession.sparkContext.getLocalProperty("moo.m2")).isEmpty)
      } finally {
        sparkSession.sparkContext.setLocalProperty("moo.m1", None.orNull)
        sparkSession.sparkContext.setLocalProperty("moo.m2", None.orNull)
      }
    }
  }

  describe("SparkConfigurationSteps - Job Group") {
    it("should set a job group") {
      SparkConfigurationSteps.setJobGroup("group1", "test1", None, pipelineContext)
      val initialGroupIds = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("group1")
      assert(initialGroupIds.isEmpty)
      val df = sparkSession.range(2)
      df.count()
      df.head()
      val group1Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("group1")
      assert(group1Ids.nonEmpty)
      SparkConfigurationSteps.setJobGroup("group2", "test2", None, pipelineContext)
      df.count()
      val group2Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("group2")
      assert(group2Ids.length < group1Ids.length)
    }

    it("should clear a job group") {
      SparkConfigurationSteps.setJobGroup("clear1", "test1", None, pipelineContext)
      val df = sparkSession.range(2)
      df.count()
      df.head()
      val group1Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("clear1")
      assert(group1Ids.nonEmpty)
      SparkConfigurationSteps.clearJobGroup(pipelineContext)
      df.count()
      assert(sparkSession.sparkContext.statusTracker.getJobIdsForGroup("clear1").length == group1Ids.length)
    }
  }

}
