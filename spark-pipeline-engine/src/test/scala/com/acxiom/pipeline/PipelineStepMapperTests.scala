package com.acxiom.pipeline

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite, GivenWhenThen}

class PipelineStepMapperTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen with Suite {
  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = new SparkConf()
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      ",org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

    SparkTestHelper.sparkSession = SparkSession.builder().config(SparkTestHelper.sparkConf).getOrCreate()

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  override def afterAll() {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  describe("PipelineMapperSteps - map parameter") {
    val classMap = Map[String, Any]("string" -> "fred", "num" -> 3)
    val globalTestObject = TestObject(1, "2", boolField=false, Map("globalTestKey1" -> "globalTestValue1"))
    val pipelineParameters = PipelineParameters(List(
      PipelineParameter("pipeline-id-1",
        Map(
          "rawKey1" -> "rawValue1",
          "red" -> None,
          "step1" -> PipelineStepResponse(
            Some(Map("primaryKey1String" -> "primaryKey1Value", "primaryKey1Map" -> Map("childKey1Integer" -> 2))),
            Some(Map("namedKey1String" -> "namedValue1", "namedKey1Boolean" -> true, "nameKey1List" -> List(0,1,2)))
          ),
          "step2" -> PipelineStepResponse(None, Some(
            Map("namedKey2String" -> "namedKey2Value",
              "namedKey2Map" -> Map(
                "childKey2String" -> "childValue2",
                "childKey2Integer" -> 2,
                "childKey2Map" -> Map("grandChildKey1Boolean" -> true)
              )
            )
          ))
        )
      ),
      PipelineParameter("pipeline-id-2", Map("rawInteger" -> 2, "rawDecimal" -> 15.65)),
      PipelineParameter("pipeline-id-3", Map("rawInteger" -> 3, "step1" -> PipelineStepResponse(Some(List(1,2,3)),
        Some(Map("namedKey" -> "namedValue")))))
    ))

    val globalParameters = Map("pipelineId" -> "pipeline-id-3", "globalString" -> "globalValue1", "globalInteger" -> 3,
      "globalBoolean" -> true, "globalTestObject" -> globalTestObject)

    val pipelineContext = PipelineContext(
      None, None, Some(globalParameters), PipelineSecurityManager(), pipelineParameters, None, PipelineStepMapper(), None, None
    )

    it("should pull the appropriate value for given parameters") {
      val tests = List(
        ("script", Parameter(value=Some("my_script"),`type`=Some("script")), "my_script"),
        ("boolean", Parameter(value=Some(true),`type`=Some("boolean")), true),
        ("cast to a boolean", Parameter(value=Some("true"),`type`=Some("boolean")), true),
        ("int", Parameter(value=Some(1),`type`=Some("integer")), 1),
        ("cast to an int", Parameter(value=Some("15"),`type`=Some("integer")), 15),
        ("big int", Parameter(value=Some(BigInt("5")),`type`=Some("integer")), BigInt("5")),
        ("decimal", Parameter(value=Some(BigInt("5")),`type`=Some("integer")), BigInt("5")),
        ("list", Parameter(value=Some(List("5")),`type`=Some("integer")), List("5")),
        ("string list", Parameter(value=Some("[\"a\", \"b\", \"c\" \"d\"]"),`type`=Some("list")), List("a", "b", "c", "d")),
        ("int list", Parameter(value=Some("[1, 2, 3]"),`type`=Some("list")), List(1, 2, 3)),
        ("default value", Parameter(name = Some("fred"), defaultValue=Some("default value"),`type`=Some("string")), "default value"),
        ("string from global", Parameter(value=Some("!globalString"),`type`=Some("string")), "globalValue1"),
        ("boolean from global", Parameter(value=Some("!globalBoolean"),`type`=Some("boolean")), true),
        ("integer from global", Parameter(value=Some("!globalInteger"),`type`=Some("integer")), 3),
        ("test object from global", Parameter(value=Some("!globalTestObject"),`type`=Some("string")), globalTestObject),
        ("child object from global", Parameter(value=Some("!globalTestObject.intField"),`type`=Some("integer")), globalTestObject.intField),
        ("non-step value from pipeline", Parameter(value=Some("$pipeline-id-1.rawKey1"),`type`=Some("string")), "rawValue1"),
        ("integer from current pipeline", Parameter(value=Some("$rawInteger"),`type`=Some("integer")), 3),
        ("missing global parameter", Parameter(value=Some("!fred"),`type`=Some("string")), None),
        ("missing runtime parameter", Parameter(value=Some("$fred"),`type`=Some("string")), None),
        ("None runtime parameter", Parameter(name = Some("red test"), value=Some("$pipeline-id-1.red"),`type`=Some("string")), None),
        ("integer from specific pipeline", Parameter(value=Some("$pipeline-id-2.rawInteger"),`type`=Some("integer")), 2),
        ("decimal from specific pipeline", Parameter(value=Some("$pipeline-id-2.rawDecimal"),`type`=Some("decimal")), 15.65),
        ("primary from current pipeline using @", Parameter(value=Some("@step1"),`type`=Some("string")), List(1,2,3)),
        ("primary from current pipeline using $", Parameter(value=Some("$step1.primaryReturn"),`type`=Some("string")), List(1,2,3)),
        ("primary from specific pipeline using @", Parameter(value=Some("@pipeline-id-1.step1.primaryKey1String"),`type`=Some("string")),
          "primaryKey1Value"),
        ("primary from specific pipeline using $", Parameter(value=Some("$pipeline-id-1.step1.primaryReturn.primaryKey1String"),`type`=Some("string")),
        "primaryKey1Value"),
        ("namedReturns from specific pipeline using $", Parameter(value=Some("$pipeline-id-1.step2.namedReturns.namedKey2String"), `type`=Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using #", Parameter(value=Some("#pipeline-id-1.step2.namedKey2String"),`type`=Some("string")), "namedKey2Value"),
        ("namedReturns from specific pipeline using # to be None", Parameter(value=Some("#pipeline-id-1.step2.nothing"),`type`=Some("string")), None),
        ("namedReturns from current pipeline using #", Parameter(value=Some("#step1.namedKey"),`type`=Some("string")), "namedValue"),
        ("resolve case class", Parameter(value=Some(classMap), className = Some("com.acxiom.pipeline.ParameterTest")), ParameterTest(Some("fred"), Some(3))),
        ("resolve map", Parameter(value=Some(classMap)), classMap)
      )

      tests.foreach(test => {
        Then(s"test ${test._1}")
        assert(pipelineContext.parameterMapper.mapParameter(test._2, pipelineContext) == test._3)
      })

      val thrown = intercept[RuntimeException] {
        pipelineContext.parameterMapper.mapParameter(Parameter(name = Some("badValue"), value=Some(1L),`type`=Some("long")), pipelineContext)
      }
      assert(Option(thrown).isDefined)
      val msg = "Unsupported value type class java.lang.Long for badValue!"
      assert(thrown.getMessage == msg)
      assert(pipelineContext.parameterMapper.mapParameter(
        Parameter(name = Some("badValue"), value=None,`type`=Some("string")), pipelineContext).asInstanceOf[Option[_]].isEmpty)
    }

    it("Should create a parameter map") {
      val emptyMap = pipelineContext.parameterMapper.createStepParameterMap(PipelineStep(), pipelineContext)
      assert(emptyMap.isEmpty)

      val parameterMap = pipelineContext.parameterMapper.createStepParameterMap(
        PipelineStep(None, None, None, None,
          Some(List(
            Parameter(name = Some("One"), value=Some(classMap), className = Some("com.acxiom.pipeline.ParameterTest")),
            Parameter(name = Some("Two"), value=Some("15"),`type`=Some("integer")),
            Parameter(name = Some("Three")),
            Parameter(name = Some("Four"), defaultValue = Some("Four default"))))),
        pipelineContext)
      assert(parameterMap.size == 4)
      assert(parameterMap.contains("One"))
      assert(parameterMap.contains("Two"))
      assert(parameterMap.contains("Three"))
      assert(parameterMap.contains("Four"))
      assert(parameterMap("One").isInstanceOf[ParameterTest])
      assert(parameterMap("One").asInstanceOf[ParameterTest].string.getOrElse("") == "fred")
      assert(parameterMap("One").asInstanceOf[ParameterTest].num.getOrElse(0) == 3)
      assert(parameterMap("Two").asInstanceOf[Int] == 15)
      assert(parameterMap("Three").asInstanceOf[Option[_]].isEmpty)
      assert(parameterMap("Four").asInstanceOf[String] == "Four default")
    }
  }
}

case class ParameterTest(string: Option[String], num: Option[Int])
case class TestObject(intField: Integer, stringField: String, boolField: Boolean, mapField: Map[String, Any])
