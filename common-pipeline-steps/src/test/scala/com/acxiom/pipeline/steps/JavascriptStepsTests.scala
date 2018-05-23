package com.acxiom.pipeline.steps

import java.io.File
import java.nio.file.{FileSystems, Files, Path, StandardCopyOption}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class JavascriptStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  val MASTER = "local[2]"
  val APPNAME = "javascript-steps-spark"
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

  describe("JavascriptSteps - Basic scripting") {
    // Copy file
    val tempFile = File.createTempFile("testFile", ".csv")
    tempFile.deleteOnExit()
    Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
      FileSystems.getDefault.getPath(tempFile.getAbsolutePath),
      StandardCopyOption.REPLACE_EXISTING)
    it("Should load a file") {
      val script =
        s"""
           |var MetaData = Java.type('org.apache.spark.sql.types.Metadata');
           |var StructType = Java.type('org.apache.spark.sql.types.StructType');
           |var StructField = Java.type('org.apache.spark.sql.types.StructField');
           |var DataTypes = Java.type('org.apache.spark.sql.types.DataTypes');
           |var schema = new StructType(new Array(
           |  new StructField('id', DataTypes.LongType, true, MetaData.empty()),
           |  new StructField('first_name', DataTypes.StringType, true, MetaData.empty()),
           |  new StructField('last_name', DataTypes.StringType, true, MetaData.empty()),
           |  new StructField('email', DataTypes.StringType, true, MetaData.empty()),
           |  new StructField('gender', DataTypes.StringType, true, MetaData.empty()),
           |  new StructField('ein', DataTypes.StringType, true, MetaData.empty()),
           |  new StructField('postal_code', DataTypes.StringType, true, MetaData.empty())
           |));
           |var sparkSession = pipelineContext.sparkSession().get();
           |var dfReader = sparkSession.read();
           |dfReader = dfReader.schema(schema).option('sep', ',').option("inferSchema", false)
           |dfReader.option("header", true).format('csv').load('${tempFile.getAbsolutePath}');
         """.stripMargin
      val result = JavascriptSteps.processScriptWithValue(script, tempFile.getAbsolutePath, pipelineContext)
      assert(result.primaryReturn.isDefined)
      val df = result.primaryReturn.get.asInstanceOf[DataFrame]
      val count = df.count()
      assert(count == 1000)
      assert(df.schema.fields.length == 7)
    }
  }
}
