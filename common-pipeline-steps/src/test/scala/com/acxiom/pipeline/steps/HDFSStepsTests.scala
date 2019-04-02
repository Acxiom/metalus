package com.acxiom.pipeline.steps

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

import scala.io.Source

class HDFSStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "hdfs-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  var config: HdfsConfiguration = _
  var fs: FileSystem = _
  var miniCluster: MiniDFSCluster = _
  val file = new File(sparkLocalDir.toFile.getAbsolutePath, "cluster")

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    // set up mini hadoop cluster
    config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, file.getAbsolutePath)
    miniCluster = new MiniDFSCluster.Builder(config).build()
    fs = FileSystem.get(config)

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
    miniCluster.shutdown()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("HDFS Steps - Basic Writing") {
    val chickens = Seq(
      ("1", "silkie"),
      ("2", "polish"),
      ("3", "sultan")
    )

    it("should successfully write to hdfs") {
      val spark = this.sparkSession
      import spark.implicits._

      val dataFrame = chickens.toDF("id", "chicken")

      HDFSSteps.writeToPath(dataFrame = dataFrame,
        path = miniCluster.getURI + "/data/chickens.csv",
        options = DataFrameWriterOptions(format = "csv")
      )
      val list = readHDFSContent(fs, miniCluster.getURI + "/data/chickens.csv")

      assert(list.size == 3)

      var writtenData: Seq[(String, String)] = Seq()
      list.foreach(l => {
        val tuple = l.split(',')
        writtenData = writtenData ++ Seq((tuple(0), tuple(1)))
      })

      writtenData.sortBy(t => t._1)

      assert(writtenData == chickens)
    }
    it("should respect options"){
      val spark = this.sparkSession
      import spark.implicits._

      val dataFrame = chickens.toDF("id", "chicken")

      HDFSSteps.writeToPath(
        dataFrame = dataFrame,
        options = DataFrameWriterOptions(format = "csv", options = Some(Map[String, String]("delimiter" -> "þ"))),
        path=miniCluster.getURI + "/data/chickens.csv"
      )
      val list = readHDFSContent(fs, miniCluster.getURI + "/data/chickens.csv")

      assert(list.size == 3)

      var writtenData: Seq[(String, String)] = Seq()
      list.foreach(l => {
        val tuple = l.split('þ')
        writtenData = writtenData ++ Seq((tuple(0), tuple(1)))
      })

      writtenData.sortBy(t => t._1)

      assert(writtenData == chickens)
    }
  }

  describe("HDFS Steps - Basic Reading"){
    val chickens = Seq(
      ("1", "silkie"),
      ("2", "polish"),
      ("3", "sultan")
    )
    it("should successfully read from hdfs") {
      val csv = "1,silkie\n2,polish\n3,sultan"
      val path = miniCluster.getURI + "/data/chickens2.csv"

      writeHDFSContext(fs, path, csv)

      val dataFrame = HDFSSteps.readFromPath(path = path,
        options = DataFrameReaderOptions(format = "csv"),
        pipelineContext = pipelineContext)

      assert(dataFrame.count() == 3)
      val result = dataFrame.take(3).map(r => (r.getString(0), r.getString(1))).toSeq
      assert(result == chickens)
    }
    it("should successfully load multiple files") {
      val csv1 = "id,chicken\n1,silkie\n2,polish"
      val csv2 = "id,chicken\n3,sultan"
      val p1 = miniCluster.getURI + "/data/c1.csv"
      val p2 = miniCluster.getURI + "/data/c2.csv"
      writeHDFSContext(fs, p1, csv1)
      writeHDFSContext(fs, p2, csv2)

      val dataFrame = HDFSSteps.readFromPaths(
        List(p1, p2),
        DataFrameReaderOptions(
          format = "csv",
          options = Some(Map[String, String]("header" -> "true"))),
        pipelineContext)
      assert(dataFrame.count() == 3)
      val result = dataFrame.take(3).map(r => (r.getString(0), r.getString(1))).toSeq.sortBy(t => t._1)
      assert(result == chickens)
    }
    it("should respect options") {
      val csv = "idþchicken\n1þsilkie\n2þpolish\n3þsultan"
      val path = miniCluster.getURI + "/data/chickens2.csv"

      writeHDFSContext(fs, path, csv)

      val dataFrame = HDFSSteps.readFromPath(
        path = path,
        options = DataFrameReaderOptions(
          format = "csv",
          options = Some(Map[String, String]("header" -> "true", "delimiter" -> "þ"))
        ),
        pipelineContext = pipelineContext)

      assert(dataFrame.count() == 3)
      val result = dataFrame.take(3).map(r => (r.getString(0), r.getString(1))).toSeq
      assert(result == chickens)
    }
  }

  private def readHDFSContent(fs: FileSystem, path: String): List[String] = {
    assert(fs.exists(new org.apache.hadoop.fs.Path(path)))

    val statuses = fs.globStatus(new org.apache.hadoop.fs.Path(path + "/part*"))
    statuses.foldLeft(List[String]())((list, stat) => list ::: Source.fromInputStream(fs.open(stat.getPath)).getLines.toList)
  }

  private def writeHDFSContext(fs: FileSystem, path: String, content: String): Unit = {

    val pw = new PrintWriter(fs.create(new org.apache.hadoop.fs.Path(path)))
    pw.print(content)
    pw.close()

  }
}
