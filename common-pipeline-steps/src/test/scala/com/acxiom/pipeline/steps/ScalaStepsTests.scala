package com.acxiom.pipeline.steps

import java.io.File
import java.nio.file.{FileSystems, Files, Path, StandardCopyOption}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class ScalaStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  val MASTER = "local[2]"
  val APPNAME = "scala-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")

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

  describe("ScalaSteps - Basic scripting") {
    // Copy file
    val tempFile = File.createTempFile("testFile", ".csv")
    tempFile.deleteOnExit()
    Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
      FileSystems.getDefault.getPath(tempFile.getAbsolutePath),
      StandardCopyOption.REPLACE_EXISTING)

    val script =
      """
        |import org.apache.spark.sql.types.Metadata
        |import org.apache.spark.sql.types.StructType
        |import org.apache.spark.sql.types.StructField
        |import org.apache.spark.sql.types.DataTypes
        |import org.apache.spark.sql._
        |val schema = StructType(List[StructField](
        |  StructField("id", DataTypes.LongType, true, Metadata.empty),
        |  StructField("first_name", DataTypes.StringType, true, Metadata.empty),
        |  StructField("last_name", DataTypes.StringType, true, Metadata.empty),
        |  StructField("email", DataTypes.StringType, true, Metadata.empty),
        |  StructField("gender", DataTypes.StringType, true, Metadata.empty),
        |  StructField("ein", DataTypes.StringType, true, Metadata.empty),
        |  StructField("postal_code", DataTypes.StringType, true, Metadata.empty)
        |))
        |val sparkSession = pipelineContext.sparkSession.get
        |var dfReader = sparkSession.read
        |dfReader = dfReader.schema(schema).option("sep", ",").option("inferSchema", false)
        |dfReader.option("header", true).format("csv").load($path)
      """.stripMargin

    it("Should load a file using Scala") {
      val updatedScript = script.replaceAll("\\$path", "\"" + tempFile.getAbsolutePath + "\"")
      val result = ScalaSteps.processScript(updatedScript, pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[DataFrame]
      val count = df.count()
      assert(count == 1000)
      assert(df.schema.fields.length == 7)
    }

    it ("Should load a file using Scala and a provide user value") {
      val updatedScript = script.replaceAll("\\$path", "userValue.asInstanceOf[String]")
      val result = ScalaSteps.processScriptWithValue(updatedScript, tempFile.getAbsolutePath, pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[DataFrame]
      val count = df.count()
      assert(count == 1000)
      assert(df.schema.fields.length == 7)
    }
  }

}
