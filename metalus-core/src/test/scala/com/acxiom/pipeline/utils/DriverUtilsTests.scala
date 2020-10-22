package com.acxiom.pipeline.utils

import com.acxiom.pipeline._
import com.acxiom.pipeline.drivers.{DriverSetup, ResultSummary, StreamingDataParser}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import scala.collection.JavaConverters._

class DriverUtilsTests extends FunSpec with BeforeAndAfterAll {

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

  describe("DriverUtils - parsePipelineJson") {
    implicit val formats: Formats = DefaultFormats

    it("Should parse a basic pipeline json returning a list of Pipeline objects") {
      val json = Serialization.write(PipelineDefs.TWO_PIPELINE)
      val pipelineList = DriverUtils.parsePipelineJson(json)
      assert(pipelineList.isDefined)
      assert(pipelineList.get.lengthCompare(2) == 0)
      verifyParsedPipelines(pipelineList.get.head, PipelineDefs.TWO_PIPELINE.head)
      verifyParsedPipelines(pipelineList.get(1), PipelineDefs.TWO_PIPELINE(1))
    }

    def verifyParsedPipelines(pipeline1: Pipeline, pipeline2: Pipeline): Unit = {
      assert(pipeline1.name.getOrElse("") == pipeline2.name.getOrElse("NONE"))
      assert(pipeline1.steps.isDefined)
      val steps = pipeline1.steps.get
      val steps1 = pipeline2.steps.get
      assert(steps.lengthCompare(steps.length) == 0)
      steps.foreach(step => {
        val step1 = steps1.find(s => s.id.getOrElse("") == step.id.getOrElse("NONE"))
        assert(step1.isDefined)
        assert(step.displayName.getOrElse("") == step1.get.displayName.getOrElse("NONE"))
        assert(step.`type`.getOrElse("") == step1.get.`type`.getOrElse("NONE"))
        assert(step.params.isDefined)
        assert(step.params.get.lengthCompare(step1.get.params.get.length) == 0)
      })
    }
  }

  describe("DriverUtils - JSON conversion") {
    implicit val formats: Formats = DefaultFormats
    it("Should convert JSON to case class") {
      val json =
        """
          |{
          | "string": "Frederico"
          |}
        """.stripMargin
      val mc = DriverUtils.parseJson(json, "com.acxiom.pipeline.MockClass")
      assert(Option(mc).isDefined)
      assert(mc.asInstanceOf[MockClass].string == "Frederico")
    }
  }

  describe("DriverUtils - Helper Functions") {
    val sparkSession = SparkSession.builder.config(new SparkConf().setMaster("local[1]")).getOrCreate()
    val df = sparkSession.createDataFrame(List(List("1", "2", "3", "4", "5"),
      List("6", "7", "8", "9", "10"),
      List("11", "12", "13", "14", "15"),
      List("16", "17", "18", "19", "20"),
      List("21", "22", "23", "24", "25")).map(r => Row(r: _*)).asJava,
      StructType(Seq(
        StructField("col1", StringType),
        StructField("col2", StringType),
        StructField("col3", StringType),
        StructField("col4", StringType),
        StructField("col5", StringType))))
    it("Should Post Initial DataFrame to Execution Plan") {
      val pipelineContext1 = PipelineContext(globals = Some(Map[String, Any]()),
        parameters = PipelineParameters(), stepMessages = None)
      val pipelineContext2 = PipelineContext(globals = Some(Map[String, Any]()),
        parameters = PipelineParameters(), stepMessages = None)
      val executionPlan = List(PipelineExecution("ONE", List(), None, pipelineContext1, None),
        PipelineExecution("TWO", List(), None, pipelineContext2, Some(List("ONE"))))
      var passed = false
      DriverUtils.processExecutionPlan(TestDriverSetup(Map(), df), executionPlan, Some(df), () => {
        passed = true
      },
        throwExceptionOnFailure = true, 1, 0)
      assert(passed, "DataFrame was not injected into PipelineContext Globals!")
    }

    it("Should load StreamingDataParsers") {
      val parameters = Map[String, Any]("streaming-parsers" -> "com.acxiom.pipeline.utils.TestStreamingDataParser")
      val parsers = StreamingUtils.generateStreamingDataParsers(parameters, Some(List[StreamingDataParser[List[String]]]()))
      assert(parsers.nonEmpty)
      assert(parsers.length == 1)
      assert(parsers.head.isInstanceOf[TestStreamingDataParser])
      val parser = StreamingUtils.getStreamingParser(sparkSession.sparkContext.emptyRDD, parsers)
      assert(parser.isDefined)
      assert(parser.get.isInstanceOf[TestStreamingDataParser])
      assert(StreamingUtils.generateStreamingDataParsers(Map[String, Any]()).isEmpty)
    }
  }

  describe("DriverUtils - Parse execution results") {
    val context = PipelineContext(globals = Some(Map("pipelineId" -> "p1", "executionId" -> "e1")), parameters = PipelineParameters(), stepMessages = None)
    it ("Should report true when results are empty") {
      assert(DriverUtils.handleExecutionResult(None).success)
    }

    it ("Should parse a result map") {
      assert(DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
        None, None)))).success)
    }

    it("Should throw an exception") {
      val message = "Should have been thrown"
      val thrown = intercept[RuntimeException] {
        DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
          "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
            None, Some(new IllegalArgumentException(message)))))).success
      }
      assert(thrown.getMessage == message)
    }

    it ("Should report unsuccessful execution") {
      val result = DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
          Some(PipelineExecutionResult(context, success = false, paused = false, None)), None))))
      assert(!result.success)
      assert(result.failedExecution.getOrElse("") == "e1")
      assert(result.failedPipeline.getOrElse("") == "p1")
    }

    it ("Should report successful execution when pipeline is paused") {
      val result = DriverUtils.handleExecutionResult(Some(Map[String, DependencyResult](
        "one" -> DependencyResult(PipelineExecution("one", List(), None, None.orNull, None),
          Some(PipelineExecutionResult(context, success = false, paused = true, None)), None))))
      assert(result.success)
    }
  }
}

class TestStreamingDataParser extends StreamingDataParser[List[String]] {
  override def canParse(rdd: RDD[List[String]]): Boolean = true

  override def parseRDD(rdd: RDD[List[String]], sparkSession: SparkSession): DataFrame = {
    None.orNull
  }
}

case class TestDriverSetup(parameters: Map[String, Any], dataFrame: DataFrame) extends DriverSetup {
  override def pipelineContext: PipelineContext = None.orNull

  override def handleExecutionResult(results: Option[Map[String, DependencyResult]]): ResultSummary = {
    ResultSummary(success = results.isDefined &&
      results.get.head._2.execution.pipelineContext.getGlobal("initialDataFrame").isDefined &&
      results.get.head._2.execution.pipelineContext.getGlobal("initialDataFrame").get == dataFrame, None, None)
  }
}
