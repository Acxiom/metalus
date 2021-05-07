package com.acxiom.delta.steps

import java.io.File
import java.nio.file.{Files, Path}

import com.acxiom.pipeline.steps.DeltaLakeSteps
import com.acxiom.pipeline.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineParameters, PipelineSecurityManager, PipelineStepMapper, PipelineStepMessage}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class DeltaLakeStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "delta-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  var config: HdfsConfiguration = _
  var fs: FileSystem = _
  var miniCluster: MiniDFSCluster = _
  val file = new File(sparkLocalDir.toFile.getAbsolutePath, "cluster")

  val data = Seq(
    (1, "cogburn", "gamecock"),
    (2, "honey", "buttercup"),
    (3, "pepper", "sex-link"),
    (4, "sesame", "cochin")
  )

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    // set up mini hadoop cluster
    config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, file.getAbsolutePath)
    miniCluster = new MiniDFSCluster.Builder(config).build()
    miniCluster.waitActive()
    // Only pull the fs object from the mini cluster
    fs = miniCluster.getFileSystem

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
      // Force Spark to use the HDFS cluster
      .set("spark.hadoop.fs.defaultFS", miniCluster.getFileSystem().getUri.toString)
    // Create the session
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
    miniCluster.shutdown()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("Deltalake - Update") {
    it("should update a delta table") {
      Given("a delta table")
      val path = s"${miniCluster.getURI}/data/chickens.delta"
      val spark = sparkSession
      import spark.implicits._
      data.toDF("id", "name", "breed").write
        .mode("overwrite")
        .format("delta")
        .save(path)
      When("an update is performed")
      DeltaLakeSteps.updateSingle(path, "breed", "'orpington'", Some("breed = 'buttercup'"), pipelineContext)
      val result = spark.read.format("delta").load(path).where("breed = 'orpington'")
      assert(result.count == 1)
      assert(result.select("name").head().getString(0) == "honey")
    }
  }

}
