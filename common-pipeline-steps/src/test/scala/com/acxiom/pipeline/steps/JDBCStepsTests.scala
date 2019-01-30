package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}
import collection.JavaConversions.propertiesAsScalaMap

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}
import java.sql.DriverManager

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.mutable

class JDBCStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "jdbc-steps-spark"
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

    System.setProperty("derby.system.home", sparkLocalDir.toFile.getAbsolutePath + "/.derby")
    val con = DriverManager.getConnection("jdbc:derby:memory:test;user=test_fixture;create=true")
    val st = con.createStatement()

    st.executeUpdate("CREATE TABLE T (ID INT PRIMARY KEY, NAME VARCHAR(100), COLOR VARCHAR(30))")
    st.executeUpdate("INSERT INTO T VALUES (1, 'SILKIE', 'WHITE')")
    st.executeUpdate("INSERT INTO T VALUES (2, 'POLISH', 'BUFF')")
    st.executeUpdate("INSERT INTO T VALUES (3, 'SULTAN', 'WHITE')")
    st.close()
    con.close()

  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  def tmp(s: String*): Unit = {

  }

  describe("JDBCSteps - Basic reading") {

    val jDBCOptions = mutable.Map[String, String]()

    jDBCOptions.put("url", "jdbc:derby:memory:test")
    jDBCOptions.put("driver", "org.apache.derby.jdbc.EmbeddedDriver")
    jDBCOptions.put("user", "test_fixture")
    jDBCOptions.put("dbtable", "T")

    it("should read a dataframe containing all records") {
      val df = JDBCSteps.read(jdbcOptions = new JDBCOptions(jDBCOptions.toMap), pipelineContext = pipelineContext)
      val count = df.count()
      assert(count == 3)
    }
    it("should respect the 'where' option") {
      val df = JDBCSteps.read(jdbcOptions = new JDBCOptions(jDBCOptions.toMap), where = Some("COLOR = 'WHITE'"), pipelineContext = pipelineContext)
      val count = df.count()
      assert(count == 2)
    }
    it("should respect columns parameter") {
      val df = JDBCSteps.read(
        jdbcOptions = new JDBCOptions(jDBCOptions.toMap),
        columns = List("NAME", "COLOR"),
        where = Some("NAME = 'POLISH'"),
        pipelineContext = pipelineContext
      )

      val count = df.columns.length
      assert(count == 2)
    }

  }

}
