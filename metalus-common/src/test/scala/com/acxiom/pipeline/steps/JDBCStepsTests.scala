package com.acxiom.pipeline.steps

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import scala.collection.mutable

class JDBCStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "jdbc-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val FOUR = 4
  val url = "jdbc:derby:memory:test"

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
    val con = DriverManager.getConnection(s"$url;user=test_fixture;create=true")
    val st = con.createStatement()

    st.executeUpdate("CREATE TABLE CHICKEN (ID INT PRIMARY KEY, NAME VARCHAR(100), COLOR VARCHAR(30))")
    st.executeUpdate("INSERT INTO CHICKEN VALUES (1, 'SILKIE', 'WHITE')")
    st.executeUpdate("INSERT INTO CHICKEN VALUES (2, 'POLISH', 'BUFF')")
    st.executeUpdate("INSERT INTO CHICKEN VALUES (3, 'SULTAN', 'WHITE')")
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

    jDBCOptions.put("url", url)
    jDBCOptions.put("driver", "org.apache.derby.jdbc.EmbeddedDriver")
    jDBCOptions.put("user", "test_fixture")
    jDBCOptions.put("dbtable", "(SELECT NAME, COLOR FROM CHICKEN) t1")

    it("should read a dataframe containing all records") {
      val df = JDBCSteps.readWithJDBCOptions(jdbcOptions = new JDBCOptions(jDBCOptions.toMap), pipelineContext = pipelineContext)
      val count = df.count()
      assert(count == 3)
    }
    it("should work using JDBStepsOptions") {
      val properties = Map[String, String](
        "user" -> "test_fixture",
        "driver" -> "org.apache.derby.jdbc.EmbeddedDriver"
      )
      val df = JDBCSteps.readWithStepOptions(JDBCDataFrameReaderOptions(
        url = url,
        table = "(SELECT NAME, COLOR FROM CHICKEN) t1",
        Some(List("COLOR = 'WHITE'", "COLOR = 'BUFF'")),
        readerOptions = DataFrameReaderOptions("jdbc", Some(properties))),
        pipelineContext = pipelineContext
      )

      val columns = df.columns.length
      val count = df.count
      assert(columns == 2)
      assert(count == 3)
      // validate that the predicates used created the correct number of partitions.
      assert(df.rdd.getNumPartitions == 2)
    }
    it("should work using properties") {
      val properties = Map[String, String](
        "user" -> "test_fixture",
        "driver" -> "org.apache.derby.jdbc.EmbeddedDriver"
      )
      val df = JDBCSteps.readWithProperties(
        url = url,
        table = "(SELECT NAME, COLOR FROM CHICKEN) t1",
        connectionProperties = Some(properties),
        pipelineContext = pipelineContext
      )

      val columns = df.columns.length
      val count = df.count
      assert(columns == 2)
      assert(count == 3)
    }

  }

  describe("JDBCSteps - Basic writing") {

    val jDBCOptions = mutable.Map[String, String]()

    jDBCOptions.put("url", url)
    jDBCOptions.put("driver", "org.apache.derby.jdbc.EmbeddedDriver")
    jDBCOptions.put("user", "test_fixture")
    jDBCOptions.put("dbtable", "CHICKENS")

    it("should be able to write to jdbc") {
      val spark = this.sparkSession
      import spark.implicits._

      val chickens = Seq(
        (4, "ONAGADORI", "WHITE"),
        (5, "APPENZELLER SPITZHAUBEN", "SILVER")
      )

      JDBCSteps.writeWithJDBCOptions(
        dataFrame = chickens.toDF("ID", "NAME", "COLOR"),
        jdbcOptions = new JDBCOptions(jDBCOptions.toMap),
        "Overwrite",
        pipelineContext
      )
      verifyCount(count = 2)
    }

    it("should respect JDBCStepsOptions") {
      val spark = this.sparkSession
      import spark.implicits._

      val chickens = Seq(
        (5, "APPENZELLER SPITZHAUBEN", "SILVER")
      )

      JDBCSteps.writeWithStepOptions(
        dataFrame = chickens.toDF("ID", "NAME", "COLOR"),
        jDBCStepsOptions = JDBCDataFrameWriterOptions(
          url = "jdbc:derby:memory:test",
          table = "CHICKENS",
          writerOptions = DataFrameWriterOptions("jdbc").setOptions(
            Map[String, String]("driver" -> "org.apache.derby.jdbc.EmbeddedDriver", "user" -> "test_fixture"))
        ),
        pipelineContext
      )
      verifyCount(count = 1)
    }
    it("should respect properties") {
      val spark = this.sparkSession
      import spark.implicits._

      val chickens = Seq(
        (4, "ONAGADORI", "WHITE"),
        (5, "APPENZELLER SPITZHAUBEN", "SILVER"),
        (6, "ROSECOMB", "BLACK"),
        (7, "SICILIAN BUTTERCUP", "BUFF")
      )

      JDBCSteps.writeWithProperties(
        dataFrame = chickens.toDF("ID", "NAME", "COLOR"),
        url = "jdbc:derby:memory:test",
        table = "CHICKENS",
        connectionProperties = Some(Map("driver" -> "org.apache.derby.jdbc.EmbeddedDriver", "user" -> "test_fixture")),
        "Overwrite",
        pipelineContext
      )
      verifyCount(FOUR)
    }
  }

  describe("JDBCSteps - Connection") {
    val options = Some(Map(
      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "user" -> "test_fixture"
    ))
    it("should get a jdbc connection") {
      val connection = JDBCSteps.getConnection(url, options)
      val st = connection.createStatement()
      val result = st.executeQuery("SELECT COUNT(*) AS CNT FROM CHICKEN")
      result.next()
      assert(result.getInt("CNT") == 3)
      JDBCSteps.closeConnection(connection)
      assert(connection.isClosed)
    }

    it("should handle a closed connection") {
      val connection = JDBCSteps.getConnection(url, options)
      connection.close()
      JDBCSteps.closeConnection(connection)
      assert(connection.isClosed)
    }

    it("should execute sql") {
      val connection = JDBCSteps.getConnection(url, options)
      val res = JDBCSteps.executeSql("SELECT COUNT(*) AS CNT FROM CHICKEN", connection)
      val resultCount = res.namedReturns.get.get("count").map(_.asInstanceOf[Int])
      val resultMap = res.primaryReturn.map(_.asInstanceOf[List[Map[String, Any]]])
      assert(resultCount.isDefined && resultCount.get == 1)
      assert(resultMap.isDefined && resultMap.get.head("CNT").asInstanceOf[Int] == 3)
      JDBCSteps.closeConnection(connection)
      assert(connection.isClosed)
    }

    it("should execute sql with parameters") {
      val connection = JDBCSteps.getConnection(url, options)
      val res = JDBCSteps.executeSql("SELECT COUNT(*) AS CNT FROM CHICKEN WHERE COLOR = ?", connection, Some(List("WHITE")))
      val resultCount = res.namedReturns.get.get("count").map(_.asInstanceOf[Int])
      val resultMap = res.primaryReturn.map(_.asInstanceOf[List[Map[String, Any]]])
      assert(resultCount.isDefined && resultCount.get == 1)
      assert(resultMap.isDefined && resultMap.get.head("CNT").asInstanceOf[Int] == 2)
      JDBCSteps.closeConnection(connection)
      assert(connection.isClosed)
    }

    it("should support updates") {
      val connection = JDBCSteps.getConnection(url, options)
      val st = connection.createStatement()
      st.executeUpdate("CREATE TABLE MORE_CHICKENS (ID INT PRIMARY KEY, NAME VARCHAR(100), COLOR VARCHAR(30))")
      st.executeUpdate("INSERT INTO MORE_CHICKENS VALUES (1, 'SILKIE', 'WHTE')")
      st.executeUpdate("INSERT INTO MORE_CHICKENS VALUES (2, 'POLISH', 'BUFF')")
      st.executeUpdate("INSERT INTO MORE_CHICKENS VALUES (3, 'SULTAN', 'WHTE')")
      st.close()
      val res = JDBCSteps.executeSql("UPDATE MORE_CHICKENS SET COLOR='WHITE' WHERE COLOR = ?", connection, Some(List("WHTE")))
      assert(res.namedReturns.get("count").asInstanceOf[Int] == 2)
      JDBCSteps.executeSql("DROP TABLE MORE_CHICKENS", connection)
      JDBCSteps.closeConnection(connection)
    }
  }

  private def verifyCount(count: Int): Unit = {
    val con = DriverManager.getConnection(s"$url;user=test_fixture")
    val st = con.createStatement()
    val result = st.executeQuery("SELECT COUNT(*) AS CNT FROM CHICKENS")
    result.next()
    assert(result.getInt("CNT") == count)
    st.close()
    con.close()
  }
}
