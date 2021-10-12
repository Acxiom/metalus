package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class CSVStepsTests extends FunSpec with BeforeAndAfterAll {

  private val MASTER = "local[2]"
  private val APPNAME = "csv-steps-spark"
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

  describe("CSVSteps - DataFrame") {
    val header = "id,moo"
    val csv = "1,m1\n2,m2\n3,m3"
    val csvWithHeader = s"$header\n$csv"

    it("Should build a dataFrame from a csv Dataset[String]") {
      val spark = sparkSession
      import spark.implicits._
      val ds = csv.split("\n").toList.toDS()
      val df = CSVSteps.csvDatasetToDataFrame(ds, None, pipelineContext)
      assert(df.count == 3)
      assert(df.columns.length == 2)
      assert(df.where(s"${df.columns(0)} = 2").collect().head.getString(1) == "m2")
    }

    it("Should build a dataFrame from a csv Dataset[String] with dataFrameReaderOptions") {
      val spark = sparkSession
      import spark.implicits._
      val ds = csvWithHeader.split("\n").toList.toDS()
      val options = Map("header" -> "true")
      val df = CSVSteps.csvDatasetToDataFrame(ds, Some(DataFrameReaderOptions("csv", Some(options))), pipelineContext)
      assert(df.count == 3)
      assert(df.columns.length == 2)
      assert(df.where(s"id = 3").collect().head.getAs[String]("moo") == "m3")
    }

    it("Should build a dataFrame from a csv string") {
      val df = CSVSteps.csvStringToDataFrame(csv, pipelineContext = pipelineContext)
      assert(df.count == 3)
      assert(df.columns.length == 2)
      assert(df.where(s"${df.columns(0)} = 2").collect().head.getString(1) == "m2")
    }

    it("Should build a data frame from a csv string and respect the header") {
      val newCsv = csvWithHeader.replaceAllLiterally(",", "\t").replaceAllLiterally("\n", "\r")
      val df = CSVSteps.csvStringToDataFrame(newCsv, Some("\t"), Some("\r"), Some(true), pipelineContext)
      assert(df.count == 3)
      assert(df.columns.length == 2)
      assert(df.where(s"id = 3").collect().head.getAs[String]("moo") == "m3")
    }
  }

}
