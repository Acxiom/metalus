package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class SparkConfigurationStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  private val MASTER = "local[2]"
  private val APPNAME = "spark-config-steps-spark"
  private var sparkConf: SparkConf = _
  private var sparkSession: SparkSession = _
  private val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  private var pipelineContext: PipelineContext = _

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
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
        SparkConfigurationSteps.setLocalProperties(Map("moo_m1" -> "m1", "moo_m2" -> "m2"), Some(true), pipelineContext)
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
        SparkConfigurationSteps.setLocalProperties(Map("moo_m1" -> None, "moo_m2" -> None), Some(true), pipelineContext)
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
      val df = sparkSession.range(2)
      df.count()
      df.head()
      val group1Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("group1")
      assert(group1Ids.length == 2)
      SparkConfigurationSteps.setJobGroup("group2", "test2", None, pipelineContext)
      df.count()
      val group2Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("group2")
      assert(group2Ids.length == 1)
    }

    it("should clear a job group") {
      SparkConfigurationSteps.setJobGroup("clear1", "test1", None, pipelineContext)
      val df = sparkSession.range(2)
      df.count()
      df.head()
      val group1Ids = sparkSession.sparkContext.statusTracker.getJobIdsForGroup("clear1")
      assert(group1Ids.length == 2)
      SparkConfigurationSteps.clearJobGroup(pipelineContext)
      df.count()
      assert(sparkSession.sparkContext.statusTracker.getJobIdsForGroup("clear1").length == 2)
    }
  }

}
