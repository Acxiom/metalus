package com.acxiom.delta.steps

import java.io.File
import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

import scala.util.Random

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
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
      val path = initTable()
      When("an update is performed")
      DeltaLakeSteps.updateSingle(path, "breed", "'orpington'", Some("breed = 'buttercup'"), pipelineContext)
      Then("The update should be applied correctly")
      val result = sparkSession.read.format("delta").load(path).where("breed = 'orpington'")
      assert(result.count == 1)
      assert(result.select("name").head().getString(0) == "honey")
    }

    it("should update multiple columns in a delta table") {
      Given("a delta table")
      val path = initTable()
      When("an update is performed")
      DeltaLakeSteps.update(path, Map("name" -> "'orange'", "breed" -> "'orpington'"), Some("breed = 'buttercup'"), pipelineContext)
      Then("The update should be applied correctly")
      val result = sparkSession.read.format("delta").load(path).where("breed = 'orpington'")
      assert(result.count == 1)
      assert(result.select("name").head().getString(0) == "orange")
    }
  }

  describe("Deltalake - Delete") {
    it("Should delete records from a delta table") {
      Given("a delta table")
      val path = initTable()
      When(" a delete is performed")
      DeltaLakeSteps.delete(path, "id = 3", pipelineContext)
      Then("the correct record should be deleted")
      val result = sparkSession.read.format("delta").load(path)
      assert(result.count == 3)
      assert(result.where("id = 3").isEmpty)
    }
  }

  describe("Deltalake - Merge") {
    it("Should upsert delta table") {
      Given("a delta table")
      val path = initTable()
      val table = sparkSession.read.format("delta").load(path)
      And("a dataframe with updated records")
      val update = table.selectExpr("id", "concat(name, '-chicken') as name", "breed")
          .union(sparkSession.sql("select 5 as id, 'orange-chicken' as name, 'orpington' as breed"))
      When("a merge is performed")
      DeltaLakeSteps.upsert(path, update, "source.id = target.id",
        pipelineContext = pipelineContext)
      val result = sparkSession.read.format("delta").load(path)
      Then("the table should contain new records")
      assert(result.count() == 5)
      And("the matched records should be updated")
      val cogburn = result.where("id = 1")
      assert(cogburn.count() == 1)
      assert(cogburn.head().getAs[String]("name") == "cogburn-chicken")
      val orange = result.where("id = 5")
      assert(orange.count() == 1)
      assert(orange.head().getAs[String]("name") == "orange-chicken")
    }

    it("Should upsert a delta table with conditions") {
      Given("a delta table")
      val path = initTable()
      val table = sparkSession.read.format("delta").load(path)
      And("a dataframe with updated records")
      val update = table.selectExpr("id", "concat(name, '-chicken') as name", "breed")
        .union(sparkSession.sql("select 5 as id, 'orange-chicken' as name, 'orpington' as breed"))
        .union(sparkSession.sql("select 6 as id, 'szechuan-chicken' as name, 'cochin' as breed"))
      DeltaLakeSteps.upsert(path, update, "s.id = t.id",
        Some("s"), Some("t"),
        Some("s.id != 3"), Some("s.id != 5"),
        pipelineContext = pipelineContext)
      val result = sparkSession.read.format("delta").load(path)
      assert(result.count() == 5)
      val pepper = result.where("id = 3")
      assert(pepper.count() == 1)
      assert(pepper.head().getAs[String]("name") == "pepper")
      assert(result.where("id = 5").isEmpty)
      val szechuan = result.where("id = 6")
      assert(szechuan.head().getAs[String]("name") == "szechuan-chicken")
    }

    it("Should merge a dataFrame with a delta table") {
      Given("a delta table")
      val path = initTable()
      val table = sparkSession.read.format("delta").load(path)
      And("a dataframe with updated records")
      val update = table.selectExpr("id", "concat(name, '-chicken') as name", "concat(breed, '-chicken') as breed")
        .union(sparkSession.sql("select 5 as id, 'orange-chicken' as name, 'orpington' as breed"))
        .union(sparkSession.sql("select 6 as id, 'szechuan-chicken' as name, 'cochin' as breed"))
      When("a merge is performed")
      DeltaLakeSteps.merge(path, update, "s.id = t.id",
        Some("s"), Some("t"),
        Some(MatchCondition(Some("t.name != 'pepper' AND t.id != 4"), Some(Map("name" -> "s.name")))),
        Some(MatchCondition(Some("t.id = 4"), None)),
        Some(MatchCondition(Some("s.id not in (4,5)"), None)),
        pipelineContext)
      Then("the table should be updated")
      val result = sparkSession.read.format("delta").load(path)
      assert(result.count() == 4)
      And("The correct columns should be updated")
      val up = result.where("name LIKE '%-chicken' AND id < 5")
      assert(up.count() == 2)
      val expected = List("cogburn-chicken", "honey-chicken")
      assert(up.select("name").collect().map(_.getString(0)).forall(expected.contains))
      And("the correct fields were not updated")
      val bad = result.where("name = 'pepper-chicken' OR breed LIKE '%-chicken'")
      assert(bad.isEmpty)
      And("the correct fields were deleted")
      assert(result.where("id = 4").isEmpty)
      And("the correct fields where inserted")
      val newRows = result.where("id > 4")
      assert(newRows.count() == 1)
      assert(newRows.head.getAs[String]("name") == "szechuan-chicken")
    }
  }

  describe("Deltalake - Meta") {
    it("Should return a history dataFrame for a delta table") {
      Given("a delta table")
      val path = initTable()
      When(" a delete is performed")
      DeltaLakeSteps.delete(path, "id = 3", pipelineContext)
      And("the history is requested")
      val history = DeltaLakeSteps.history(path, None, pipelineContext)
      Then("there should be 2 operations performed")
      assert(history.count() == 2)
      And("one of the operations should be a delete")
      assert(!history.where("operation = 'DELETE'").isEmpty)
    }

    it("Should return a history dataFrame for the last command run on a  delta table") {
      Given("a delta table")
      val path = initTable()
      When(" a delete is performed")
      DeltaLakeSteps.delete(path, "id = 3", pipelineContext)
      And("the history is requested")
      val history = DeltaLakeSteps.history(path, Some(1), pipelineContext)
      Then("there should be one result")
      assert(history.count() == 1)
      And("the operation should be a delete")
      assert(!history.where("operation = 'DELETE'").isEmpty)
    }
  }

  private def initTable(): String = {
    val characters = 9
    val rand = Random.alphanumeric.take(characters).mkString
    val path = s"${miniCluster.getURI}/data/$rand/chickens.delta"
    val spark = sparkSession
    import spark.implicits._
    data.toDF("id", "name", "breed").write
      .mode("overwrite")
      .format("delta")
      .save(path)
    path
  }

}
