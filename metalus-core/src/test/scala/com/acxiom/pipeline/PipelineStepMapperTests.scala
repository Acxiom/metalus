package com.acxiom.pipeline

import java.io.File

import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen, Suite}

class PipelineStepMapperTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen with Suite {
  private val FIVE = 5
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
          )),
          "step3" -> PipelineStepResponse(Some("fred"), None),
          "nullValue" -> None.orNull,
          "recursiveTest" -> "$pipeline-id-1.rawKey1"
        )
      ),
      PipelineParameter("pipeline-id-2", Map("rawInteger" -> 2, "rawDecimal" -> 15.65)),
      PipelineParameter("pipeline-id-3", Map("rawInteger" -> 3, "step1" -> PipelineStepResponse(Some(List(1,2,3)),
        Some(Map("namedKey" -> "namedValue")))))
    ))

    val broadCastGlobal = Map[String, Any](
      "link1" -> "!{pipelineId}::!{globalBoolean}::#{pipeline-id-1.step2.namedKey2Map.childKey2Integer}::@{pipeline-id-1.step3}",
      "link2" -> "@step1", "link3" -> "$step1.primaryReturn", "link4" -> "@pipeline-id-1.step1.primaryKey1String",
      "link5" -> "$pipeline-id-1.step1.primaryReturn.primaryKey1String", "link6" -> "$pipeline-id-1.step2.namedReturns.namedKey2String",
      "link7" -> "#pipeline-id-1.step2.namedKey2String", "link8" -> "embedded!{pipelineId}::concat_value", "link9" -> "link_override"
    )
    val globalParameters = Map("pipelineId" -> "pipeline-id-3", "lastStepId" -> "step1", "globalString" -> "globalValue1", "globalInteger" -> FIVE,
      "globalBoolean" -> true, "globalTestObject" -> globalTestObject, "GlobalLinks" -> broadCastGlobal, "link9" -> "root_value",
      "extractMethodsEnabled" -> true,
      "complexList" -> List(DefaultPipelineStepResponse(Some("moo")), DefaultPipelineStepResponse(Some("moo2"))),
      "optionsList" -> List(Some("1"), Some("2"), None, Some("3"))
    )

    val subPipeline = Pipeline(Some("mypipeline"), Some("My Pipeline"))

    val pipelineContext = PipelineContext(
      None, None, Some(globalParameters), PipelineSecurityManager(), pipelineParameters, None, PipelineStepMapper(), None, None,
      ExecutionAudit("root", AuditType.EXECUTION, Map[String, Any](), System.currentTimeMillis()), PipelineManager(List(subPipeline)))

    it("should pull the appropriate value for given parameters") {
      val tests = List(
        ("basic string concatenation missing value", Parameter(value = Some("!{bad-value}::concat_value")), "None::concat_value"),
        ("basic string concatenation", Parameter(value = Some("!{pipelineId}::concat_value")), "pipeline-id-3::concat_value"),
        ("embedded string concatenation", Parameter(value = Some("embedded!{pipelineId}::concat_value")), "embeddedpipeline-id-3::concat_value"),
        ("embedded string concatenation in GlobalLink", Parameter(value = Some("!link8")), "embeddedpipeline-id-3::concat_value"),
        ("multiple embedded string concatenation",
          Parameter(value = Some("!{pipelineId}::!{globalBoolean}::#{pipeline-id-1.step2.namedKey2Map.childKey2Integer}::@{pipeline-id-1.step3}")),
          "pipeline-id-3::true::2::fred"),
        ("multiple embedded string concatenation using BroadCast", Parameter(value = Some("!link1")), "pipeline-id-3::true::2::fred"),
        ("string concatenation object override", Parameter(value = Some("somestring::!{globalTestObject}::concat_value")), globalTestObject),
        ("script", Parameter(value = Some("my_script"), `type` = Some("script")), "my_script"),
        ("boolean", Parameter(value = Some(true), `type` = Some("boolean")), true),
        ("cast to a boolean", Parameter(value = Some("true"), `type` = Some("boolean")), true),
        ("int", Parameter(value = Some(1), `type` = Some("integer")), 1),
        ("cast to an int", Parameter(value = Some("15"), `type` = Some("integer")), 15),
        ("big int", Parameter(value = Some(BigInt("5")), `type` = Some("integer")), BigInt("5")),
        ("decimal", Parameter(value = Some(BigInt("5")), `type` = Some("integer")), BigInt("5")),
        ("list", Parameter(value = Some(List("5")), `type` = Some("integer")), List("5")),
        ("string list", Parameter(value = Some("[\"a\", \"b\", \"c\" \"d\"]"), `type` = Some("list")), List("a", "b", "c", "d")),
        ("int list", Parameter(value = Some("[1, 2, 3]"), `type` = Some("list")), List(1, 2, 3)),
        ("default value", Parameter(name = Some("fred"), defaultValue = Some("default value"), `type` = Some("string")), "default value"),
        ("string from global", Parameter(value = Some("!globalString"), `type` = Some("string")), "globalValue1"),
        ("GlobalLink overrides root global", Parameter(value = Some("!link9"), `type` = Some("string")), "link_override"),
        ("boolean from global", Parameter(value = Some("!globalBoolean"), `type` = Some("boolean")), true),
        ("integer from global", Parameter(value = Some("!globalInteger"), `type` = Some("integer")), FIVE),
        ("test object from global", Parameter(value = Some("!globalTestObject"), `type` = Some("string")), globalTestObject),
        ("child object from global", Parameter(value = Some("!globalTestObject.intField"), `type` = Some("integer")), globalTestObject.intField),
        ("non-step value from pipeline", Parameter(value = Some("$pipeline-id-1.rawKey1"), `type` = Some("string")), "rawValue1"),
        ("integer from current pipeline", Parameter(value = Some("$rawInteger"), `type` = Some("integer")), 3),
        ("missing global parameter", Parameter(value = Some("!fred"), `type` = Some("string")), None),
        ("missing runtime parameter", Parameter(value = Some("$fred"), `type` = Some("string")), None),
        ("None runtime parameter", Parameter(name = Some("red test"), value = Some("$pipeline-id-1.red"), `type` = Some("string")), None),
        ("integer from specific pipeline", Parameter(value = Some("$pipeline-id-2.rawInteger"), `type` = Some("integer")), 2),
        ("decimal from specific pipeline", Parameter(value = Some("$pipeline-id-2.rawDecimal"), `type` = Some("decimal")), 15.65),
        ("primary from current pipeline using @", Parameter(value = Some("@step1"), `type` = Some("string")), List(1, 2, 3)),
        ("primary from current pipeline using @ in GlobalLink", Parameter(value = Some("!link2"), `type` = Some("string")), List(1, 2, 3)),
        ("primary from current pipeline using $", Parameter(value = Some("$step1.primaryReturn"), `type` = Some("string")), List(1, 2, 3)),
        ("primary from current pipeline using $ in GlobalLink", Parameter(value = Some("!link3"), `type` = Some("string")), List(1, 2, 3)),
        ("primary from specific pipeline using @", Parameter(value = Some("@pipeline-id-1.step1.primaryKey1String"), `type` = Some("string")),
          "primaryKey1Value"),
        ("primary from specific pipeline using @ in GlobalLink", Parameter(value = Some("!link4"), `type` = Some("string")), "primaryKey1Value"),
        ("primary from specific pipeline using $", Parameter(value = Some("$pipeline-id-1.step1.primaryReturn.primaryKey1String"), `type` = Some("string")),
          "primaryKey1Value"),
        ("primary from specific pipeline using $ in GlobalLink", Parameter(value = Some("!link5"), `type` = Some("string")), "primaryKey1Value"),
        ("namedReturns from specific pipeline using $", Parameter(value = Some("$pipeline-id-1.step2.namedReturns.namedKey2String"), `type` = Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using $ in GlobalLink", Parameter(value = Some("!link6"), `type` = Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using #", Parameter(value = Some("#pipeline-id-1.step2.namedKey2String"), `type` = Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using #", Parameter(value = Some("!link7"), `type` = Some("string")), "namedKey2Value"),
        ("namedReturns from specific pipeline using # to be None", Parameter(value = Some("#pipeline-id-1.step2.nothing"), `type` = Some("string")), None),
        ("namedReturns from current pipeline using #", Parameter(value = Some("#step1.namedKey"), `type` = Some("string")), "namedValue"),
        ("resolve case class", Parameter(value = Some(classMap), className = Some("com.acxiom.pipeline.ParameterTest")), ParameterTest(Some("fred"), Some(3))),
        ("resolve map", Parameter(value = Some(classMap)), classMap),
        ("resolve pipeline", Parameter(value = Some("&mypipeline")), subPipeline),
        ("fail to resolve pipeline", Parameter(value = Some("&mypipeline1")), None),
        ("fail to get global string", Parameter(value = Some("!invalidGlobalString")), None),
        ("fail to detect null", Parameter(value = Some("$nullValue || default string")), "default string"),
        ("recursive test", Parameter(value = Some("?pipeline-id-1.recursiveTest")), "rawValue1"),
        ("recursive test", Parameter(value = Some("$pipeline-id-1.recursiveTest")), "$pipeline-id-1.rawKey1"),
        ("lastStepId test", Parameter(value = Some("@LastStepId"), `type` = Some("string")), List(1, 2, 3)),
        ("lastStepId with or", Parameter(value = Some("!not_here || @LastStepId"), `type` = Some("string")), List(1, 2, 3)),
        ("lastStepId with method extraction", Parameter(value = Some("@LastStepId.nonEmpty"), `type` = Some("boolean")), true),
        ("inline list in inline map",
          Parameter(value = Some(Map[String, Any]("list" -> List("!globalString"))), `type` = Some("text")), Map("list" -> List("globalValue1"))),
        ("script to map a list of step returns",
          Parameter(value = Some(
            """(list: !complexList :Option[List[com.acxiom.pipeline.DefaultPipelineStepResponse]])
              |list.getOrElse(List()).map(r => r.primaryReturn.get)
              |""".stripMargin), `type` = Some("scalascript")), List("moo", "moo2")),
        ("simple script with tuple",
          Parameter(value = Some(
            """()("moo", 1)"""), `type` = Some("scalascript")), ("moo", 1)),
        ("script with implicit",
          Parameter(value = Some(
            """(list:!optionsList:List[Option[String]])(list.flatten)"""), `type` = Some("scalascript")), List("1", "2", "3")),
        ("script with escaped binding",
          Parameter(value = Some(
            """(s:cChickens\:Rule\\:String)(s.toLowerCase.drop(1))"""), `type` = Some("scalascript")), "chickens:rule\\")

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

    it("Should throw the appropriate error when given script with malformed bindings") {
      val thrown = intercept[PipelineException] {
        pipelineContext.parameterMapper.mapParameter(Parameter(value = Some(
          """(list)list.flatten"""), `type` = Some("scalascript")), pipelineContext)
      }
      assert(thrown.getMessage == "Unable to execute script: Illegal binding format: [list]. Expected format: <name>:<value>:<type>")
    }

    it("Should throw the appropriate error when given malformed bindings") {
      val thrown = intercept[PipelineException] {
        pipelineContext.parameterMapper.mapParameter(Parameter(value = Some(
          """(chk:chicken:String chk.toLowerCase"""), `type` = Some("scalascript")), pipelineContext)
      }
      assert(thrown.getMessage == "Unable to execute script: Malformed bindings. Expected enclosing character: [)].")
    }

    it("Should throw the appropriate error when given a malformed script") {
      val thrown = intercept[PipelineException] {
        pipelineContext.parameterMapper.mapParameter(Parameter(value = Some(
          """(df:@DataFrame:DataFrame df.count()"""), `type` = Some("scalascript")), pipelineContext)
      }
      assert(thrown.getMessage == "Unable to execute script: script is empty. Ensure bindings are properly enclosed.")
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
            Parameter(name = Some("Four"), defaultValue = Some("Four default")),
            Parameter(name = Some("Five"), value=Some("silkie.chicken@chickens.com"))))),
        pipelineContext)
      assert(parameterMap.size == 5)
      assert(parameterMap.contains("One"))
      assert(parameterMap.contains("Two"))
      assert(parameterMap.contains("Three"))
      assert(parameterMap.contains("Four"))
      assert(parameterMap.contains("Five"))
      assert(parameterMap("One").isInstanceOf[ParameterTest])
      assert(parameterMap("One").asInstanceOf[ParameterTest].string.getOrElse("") == "fred")
      assert(parameterMap("One").asInstanceOf[ParameterTest].num.getOrElse(0) == 3)
      assert(parameterMap("Two").asInstanceOf[Int] == 15)
      assert(parameterMap("Three").asInstanceOf[Option[_]].isEmpty)
      assert(parameterMap("Four").asInstanceOf[String] == "Four default")
      assert(parameterMap("Five").asInstanceOf[String] == "silkie.chicken@chickens.com")
    }

    it("Should create object with variables") {
      val objectMap = Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger")
      val objectParameter = Parameter(value=Some(objectMap), className = Some("com.acxiom.pipeline.ParameterTest"))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      assert(parameterValue == ParameterTest(Some("globalValue1"), Some(FIVE)))

      val ctx = pipelineContext.setGlobal("pipelineId", "pipeline-id-1")
      val nestedObjectMap = Map[String, Any](
        "parameterTest" -> Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger"),
        "testObject" -> Map[String, Any](
          "intField" -> "@step1.primaryKey1Map.childKey1Integer", // Should be 2
          "stringField" -> "#step1.namedKey1String", // Should be namedValue1
          "boolField" -> "#step1.namedKey1Boolean", // Should be true
          "mapField" -> Map("globalTestKey1" -> "$rawKey1") // Should be rawValue1
        )
      )
      val nestedCaseClass = ctx.parameterMapper.mapParameter(
        Parameter(value=Some(nestedObjectMap),
          className = Some("com.acxiom.pipeline.NestedCaseClassTest")), ctx)
      assert(nestedCaseClass == NestedCaseClassTest(ParameterTest(Some("globalValue1"), Some(FIVE)),
        TestObject(2, "namedValue1", boolField = true, Map("globalTestKey1" -> "rawValue1"))))
    }

    it("Should replace variables in a map") {
      val objectMap = Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger", "concatString" -> "global->!{globalString}->value")
      val objectParameter = Parameter(value=Some(objectMap))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      assert(parameterValue.asInstanceOf[Map[String, Any]]("string").asInstanceOf[Option[String]].contains("globalValue1"))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("num").asInstanceOf[Option[Int]].contains(FIVE))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("concatString").asInstanceOf[Option[String]].contains("global->globalValue1->value"))
    }

    it("Should create a list of objects") {
      val objects = List(Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger"),
        Map[String, Any]("string" -> "secondObject", "num" -> (FIVE + FIVE)))
      val objectParameter = Parameter(value=Some(objects), className = Some("com.acxiom.pipeline.ParameterTest"))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[ParameterTest]]
      assert(list.length == 2)
      assert(list.head == ParameterTest(Some("globalValue1"), Some(FIVE)))
      assert(list(1) == ParameterTest(Some("secondObject"), Some(FIVE + FIVE)))
    }

    it("Should create a list of maps") {
      val objects = List(Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger"),
        Map[String, Any]("string" -> "secondObject", "num" -> (FIVE + FIVE)))
      val objectParameter = Parameter(value=Some(objects))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Map[String, Any]]]
      assert(list.length == 2)
      assert(list.head("string").asInstanceOf[Option[String]].contains("globalValue1"))
      assert(list.head("num").asInstanceOf[Option[Int]].contains(FIVE))
      assert(list(1)("string").asInstanceOf[String] == "secondObject")
      assert(list(1)("num").asInstanceOf[Int] == (FIVE + FIVE))
    }

    it("Should return a list of items") {
      val objectParameter = Parameter(value=Some(List("string", FIVE, "global->!{globalString}->value")))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Any]]
      assert(list.length == 3)
      assert(list.head == "string")
      assert(list(1) == FIVE)
      assert(list(2) == "global->globalValue1->value")
    }

    it("Should respect the dropNoneFromLists option") {
      val objectParameter = Parameter(value=Some(List("string", FIVE, "!NotHere")))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Any]]
      assert(list.length == 2)
      assert(list.head == "string")
      assert(list(1) == FIVE)
      val contextWithOption = pipelineContext.setGlobal("dropNoneFromLists", false)
      val anotherValue = pipelineContext.parameterMapper.mapParameter(objectParameter, contextWithOption)
      val anotherList = anotherValue.asInstanceOf[List[Any]]
      assert(anotherList.length == 3)
      assert(anotherList.head == "string")
      assert(anotherList(1) == FIVE)
      assert(anotherList(2) == null)
    }

    it("Should perform || logic") {
      val json =
        """
          |{
          |          "type": "text",
          |          "name": "path",
          |          "required": false,
          |          "value": "$nv || default string"
          |        }
          |""".stripMargin

      val mapJson =
        s"""
           |{
           |  "int": 1,
           |  "nv": null,
           |  "string": "some string"
           |}
           |""".stripMargin

      implicit val formats: Formats = DefaultFormats
      val params = org.json4s.native.JsonMethods.parse(mapJson).extract[Map[String, Any]]
      val param = DriverUtils.parseJson(json, "com.acxiom.pipeline.Parameter").asInstanceOf[Parameter]
      val ctx = params.foldLeft(pipelineContext)((context, entry) => {
        context.setParameterByPipelineId("test-pipeline", entry._1, entry._2)
      })
        .setGlobal("pipelineId", "test-pipeline")
      assert(ctx.parameterMapper.mapParameter(param, ctx) == "default string")
      assert(ctx.parameterMapper.mapParameter(Parameter(value = Some("$string")), ctx) == "some string")
      assert(ctx.parameterMapper.mapParameter(Parameter(value = Some("$int")), ctx) == 1)
    }
  }
}

case class NestedCaseClassTest(parameterTest: ParameterTest, testObject: TestObject)
case class ParameterTest(string: Option[String], num: Option[Int])
case class TestObject(intField: Integer, stringField: String, boolField: Boolean, mapField: Map[String, Any])
