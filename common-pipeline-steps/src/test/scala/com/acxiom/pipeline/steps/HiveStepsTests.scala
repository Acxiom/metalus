package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class HiveStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "hive-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val sparkWarehouseDir: Path = Files.createTempDirectory("sparkWarehouse")

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    System.setProperty("derby.system.home", sparkLocalDir.toFile.getAbsolutePath + "/.derby")
    System.setProperty("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory;databaseName=${sparkLocalDir.toFile.getAbsolutePath}/metastore_db;create=true")

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
      .set("spark.sql.warehouse.dir", sparkWarehouseDir.toFile.getAbsolutePath)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

    val spark = this.sparkSession
    import spark.implicits._

    Seq(
      (1, "silkie"),
      (2, "buttercup"),
      (3, "leghorn")
    ).toDF("id", "breed").write.saveAsTable("breeds")
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
    FileUtils.deleteDirectory(sparkWarehouseDir.toFile)
  }

  describe("HiveSteps - Basic IO"){

    val chickens = Seq(
      ("1", "silkie"),
      ("2", "polish"),
      ("3", "sultan")
    )

    it("should write a dataFrame to hive"){
      val spark = this.sparkSession
      import spark.implicits._
      val dataFrame = chickens.toDF("id", "chicken")
      HiveSteps.writeDataFrame(dataFrame, "chickens", DataFrameWriterOptions())

      val result = spark.sql("select * from chickens")
      assert(result.count() == 3)
    }

    it("should read a dataFrame from hive"){
      val frame = HiveSteps.readDataFrame("breeds", DataFrameReaderOptions(), pipelineContext)
      assert(frame.count() == 3)
      assert(frame.where("breed = 'leghorn'").count() == 1)
    }
  }

}
