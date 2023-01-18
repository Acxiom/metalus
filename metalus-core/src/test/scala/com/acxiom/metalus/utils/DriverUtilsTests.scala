package com.acxiom.metalus.utils

import com.acxiom.metalus._
import org.scalatest.funspec.AnyFunSpec
//import org.apache.spark.streaming.{Milliseconds, Minutes, Seconds}
import org.scalatest.BeforeAndAfterAll

class DriverUtilsTests extends AnyFunSpec with BeforeAndAfterAll {

  describe("DriverUtils - extractParameters") {
    it("Should properly parse input parameters") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val map = DriverUtils.extractParameters(params)
      assert(map("one") == "1")
      assert(map("two") == "2")
      assert(map("three").asInstanceOf[Boolean])
    }

    it("Should properly parse input parameters and fail on missing parameter") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val thrown = intercept[RuntimeException] {
        DriverUtils.extractParameters(params, Some(List("three", "four", "five")))
      }
      assert(thrown.getMessage.contains("Missing required parameters: four,five"))
    }

    it ("Should parse common parameters") {
      val params = DriverUtils.parseCommonParameters(Map("driverSetupClass" -> "test",
      "maxRetryAttempts" -> 5,
      "terminateAfterFailures" -> true))
      assert(params.initializationClass == "test")
      assert(params.maxRetryAttempts == Constants.FIVE)
      assert(params.terminateAfterFailures)
    }
  }

  describe("DriverUtils - Helper Functions") {
    // TODO [2.0 Review] Move to spark project
//    val sparkSession = SparkSession.builder.config(new SparkConf().setMaster("local[1]")).getOrCreate()
//    val df = sparkSession.createDataFrame(List(List("1", "2", "3", "4", "5"),
//      List("6", "7", "8", "9", "10"),
//      List("11", "12", "13", "14", "15"),
//      List("16", "17", "18", "19", "20"),
//      List("21", "22", "23", "24", "25")).map(r => Row(r: _*)).asJava,
//      StructType(Seq(
//        StructField("col1", StringType),
//        StructField("col2", StringType),
//        StructField("col3", StringType),
//        StructField("col4", StringType),
//        StructField("col5", StringType))))
//    it("Should Post Initial DataFrame to Execution Plan") {
//      val pipelineContext1 = PipelineContext(globals = Some(Map[String, Any]()),
//        parameters = PipelineParameters(), stepMessages = None)
//      val pipelineContext2 = PipelineContext(globals = Some(Map[String, Any]()),
//        parameters = PipelineParameters(), stepMessages = None)
//      val executionPlan = List(PipelineExecution("ONE", List(), None, pipelineContext1, None),
//        PipelineExecution("TWO", List(), None, pipelineContext2, Some(List("ONE"))))
//      var passed = false
//      DriverUtils.processExecutionPlan(TestDriverSetup(Map(), df), executionPlan, Some(df), () => {
//        passed = true
//      },
//        throwExceptionOnFailure = true, 1, 0)
//      assert(passed, "DataFrame was not injected into PipelineContext Globals!")
//      assert(DriverUtils.resultMap("results").isDefined)
//      val resultMap = DriverUtils.resultMap("results").get
//      assert(resultMap.size == 2)
//      assert(resultMap("ONE").execution.id == "ONE")
//      assert(resultMap("ONE").result.get.success)
//      assert(resultMap("TWO").execution.id == "TWO")
//      assert(resultMap("TWO").result.get.success)
//    }

//    it("Should get duration") {
//      assert(StreamingUtils.getDuration(Some("minutes"), Some("5")) == Minutes(Constants.FIVE))
//      assert(StreamingUtils.getDuration(Some("milliseconds"), Some("15")) == Milliseconds(Constants.FIFTEEN))
//      assert(StreamingUtils.getDuration(None, None) == Seconds(Constants.TEN))
//      assert(StreamingUtils.getDuration(Some("monkey"), None) == Seconds(Constants.TEN))
//    }
  }

//  describe("DriverUtils - Parse execution results") {
//    val context = PipelineContext(globals = Some(Map("pipelineId" -> "p1", "executionId" -> "e1")), parameters = PipelineParameters(), stepMessages = None)
//    it ("Should report true when results are empty") {
//      assert(DriverUtils.handleExecutionResult(None).success)
//    }
//
//    it ("Should parse a result map") {
//      assert(DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
//        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
//        None, None)))).success)
//    }
//
//    it("Should throw an exception") {
//      val message = "Should have been thrown"
//      val thrown = intercept[RuntimeException] {
//        DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
//          "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
//            None, Some(new IllegalArgumentException(message)))))).success
//      }
//      assert(thrown.getMessage == message)
//    }
//
//    it ("Should report unsuccessful execution") {
//      val result = DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
//        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
//          Some(PipelineExecutionResult(context, success = false, paused = false, None)), None))))
//      assert(!result.success)
//      assert(result.failedExecution.getOrElse("") == "e1")
//      assert(result.failedPipeline.getOrElse("") == "p1")
//    }
//
//    it ("Should report successful execution when pipeline is paused") {
//      val result = DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
//        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
//          Some(PipelineExecutionResult(context, success = false, paused = true, None)), None))))
//      assert(result.success)
//    }
//  }
}

// TODO [2.0 Review] Move this to spark project?
//case class TestDriverSetup(parameters: Map[String, Any], dataFrame: DataFrame) extends DriverSetup {
//  override def pipelineContext: PipelineContext = None.orNull
//
//  override def handleExecutionResult(results: Option[Map[String, DependencyResult]]): ResultSummary = {
//    ResultSummary(success = results.isDefined &&
//      results.get.head._2.execution.pipelineContext.getGlobal("initialDataFrame").isDefined &&
//      results.get.head._2.execution.pipelineContext.getGlobal("initialDataFrame").get == dataFrame, None, None)
//  }
//}
