package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class QueryStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  val MASTER = "local[2]"
  val APPNAME = "query-steps-spark"
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

  describe("Query Step Tests") {
    it("should allow queries to be run on existing dataframes") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483, 23.44, 4),
        (2, "rascal", "dawg", 29483, -10.41, 4),
        (3, "fluffy", "cat", 72034, -10.41, 4)
      )).toDF("id", "first_name", "last_name", "zip", "amount", "age")

      val inputDFCount = destDF.count

      Then("store a dataframe as a TempView with a provided name")
      val viewName1 = QuerySteps.dataFrameToTempView(destDF, Some("test_view_1"), this.pipelineContext)
      And("expect a tempview to exist with the name provided with the same count as the original")
      assert(viewName1 == "test_view_1")
      assert(sparkSession.sql("select * from test_view_1").count == inputDFCount)

      Then("store a dataframe as a TempView without providing a name")
      val viewName2 = QuerySteps.dataFrameToTempView(destDF, None, this.pipelineContext)
      And("expect a TempView to exist with the random name provided")
      assert(sparkSession.sql(s"select * from $viewName2").count == inputDFCount)

      Then("run a query and store the results in a named TempView")
      val viewName3 = QuerySteps.queryToTempView(s"select id, first_name, last_name from $viewName1",
        None, Some("test_view_3"), this.pipelineContext)
      And("expect a TempView to exist with the name provided")
      assert(viewName3 == "test_view_3")

      Then("run a query and store the results in a randomly named TempView")
      val viewName4 = QuerySteps.queryToTempView(s"select id, amount, zip from $viewName1",
        None, None, this.pipelineContext)
      Then("save the results of a query to a new DataFrame")
      val dataFrame4 = QuerySteps.queryToDataFrame(s"select * from $viewName4", None, this.pipelineContext)
      assert(dataFrame4.count == inputDFCount)
    }

    it("should modify a query with replacement variables") {
      val tests = List(
        TestingClass(
          "no variable replacement test (only remove semi-colons",
          "select * from ${viewName} where fieldName = '${testValue}';",
          None,
          "select * from ${viewName} where fieldName = '${testValue}'"
        ),
        TestingClass(
          "basic variable replacement test",
          "select * from ${viewName} where fieldName = '${testValue}';",
          Some(Map("viewName" -> "test_view_1", "testValue" -> "myValue")),
          "select * from test_view_1 where fieldName = 'myValue'"
        ),
        TestingClass(
          "only replaces variables when the proper identifier are used",
          "select * from $viewName where fieldName = '${testValue';",
          Some(Map("viewName" -> "test_view_1", "testValue" -> "myValue")),
          "select * from $viewName where fieldName = '${testValue'"
        )
      )

      tests.foreach(test => {
        Then(s"expect test '${test.desc}' to return the expected results")
        assert(QuerySteps.replaceQueryVariables(test.query, test.variableMap) == test.expected)
      })
    }
  }
}

case class TestingClass(desc: String, query: String, variableMap: Option[Map[String, String]], expected: String)