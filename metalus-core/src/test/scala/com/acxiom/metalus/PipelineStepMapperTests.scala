package com.acxiom.metalus

import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.parser.JsonParser
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

class PipelineStepMapperTests extends AnyFunSpec with BeforeAndAfterAll with GivenWhenThen {
  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)
  }

  override def afterAll(): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("PipelineMapperSteps - map parameter") {
    val classMap = Map[String, Any]("string" -> "fred", "num" -> 3)
    val globalTestObject = TestObject(1, "2", boolField=false, Map("globalTestKey1" -> "globalTestValue1"))
    val pipelineParameters = List(
      PipelineParameter(PipelineStateKey("pipeline-id-1"),
        Map(
          "rawKey1" -> "rawValue1",
          "red" -> None,
          "nullValue" -> None.orNull,
          "recursiveTest" -> "$pipeline-id-1.rawKey1"
        )
      ),
      PipelineParameter(PipelineStateKey("pipeline-id-2"), Map("rawInteger" -> 2, "rawDecimal" -> 15.65)),
      PipelineParameter(PipelineStateKey("pipeline-id-3"), Map("rawInteger" -> 3))
    )

    val stepResults = Map(
      PipelineStateKey("pipeline-id-1", Some("step1")) -> PipelineStepResponse(
        Some(Map("primaryKey1String" -> "primaryKey1Value", "primaryKey1Map" -> Map("childKey1Integer" -> 2))),
        Some(Map("namedKey1String" -> "namedValue1", "namedKey1Boolean" -> true, "nameKey1List" -> List(0, 1, 2)))),
      PipelineStateKey("pipeline-id-1", Some("step2")) -> PipelineStepResponse(None, Some(
        Map("namedKey2String" -> "namedKey2Value",
          "namedKey2Map" -> Map(
            "childKey2String" -> "childValue2",
            "childKey2Integer" -> 2,
            "childKey2Map" -> Map("grandChildKey1Boolean" -> true))))),
      PipelineStateKey("pipeline-id-1", Some("step3")) -> PipelineStepResponse(Some("fred"), None),
      PipelineStateKey("pipeline-id-3", Some("step1")) -> PipelineStepResponse(Some(List(1, 2, 3)),
        Some(Map("namedKey" -> "namedValue"))),
      PipelineStateKey("pipeline-id-3", Some("step3")) -> PipelineStepResponse(Some("fred_on_the_head"), None))

    val broadCastGlobal = Map[String, Any](
      "link1" -> "!{pipelineId}::!{globalBoolean}::#{pipeline-id-1.step2.namedKey2Map.childKey2Integer}::@{pipeline-id-1.step3}",
      "link2" -> "@step1", "link3" -> "@step1", "link4" -> "@pipeline-id-1.step1.primaryKey1String",
      "link5" -> "@pipeline-id-1.step1.primaryKey1String", "link6" -> "#pipeline-id-1.step2.namedKey2String",
      "link7" -> "#pipeline-id-1.step2.namedKey2String", "link8" -> "embedded!{pipelineId}::concat_value", "link9" -> "link_override"
    )
    val globalParameters = Map("pipelineId" -> "pipeline-id-3", "lastStepId" -> "step1", "globalString" -> "globalValue1", "globalInteger" -> Constants.FIVE,
      "globalBoolean" -> true, "globalTestObject" -> globalTestObject, "GlobalLinks" -> broadCastGlobal, "link9" -> "root_value",
      "complexList" -> List(PipelineStepResponse(Some("moo")), PipelineStepResponse(Some("moo2"))),
      "optionsList" -> List(Some("1"), Some("2"), None, Some("3")),
      "numericString" -> "1", "bigIntGlobal" -> BigInt(1),
      "booleanString" -> "true",
      "runtimeGlobal" -> "$rawKey1",
      "int" -> 1,
      "long" -> 1L,
      "float" -> 1.0F,
      "double" -> 1.0D,
      "byte" -> 1.toByte,
      "short" -> 1.toShort,
      "char" -> 1.toChar,
      "bigdecimal" -> BigDecimal(1),
      "string" -> "stringWithinAnOption",
      "tripleOption" -> Some(Some(Some("stringWithinAnOption"))),
      "pipelinesMap" -> List(Map("pipelineId" -> "30273bbb-983b-43ad-a549-6d23f3c0d143"))
    )

    val subPipeline = Pipeline(Some("mypipeline"), Some("My Pipeline"))
    val credentialProvider = new DefaultCredentialProvider(
      Map[String, Any]("credential-classes" -> "com.acxiom.metalus.DefaultCredential",
      "credentialName" -> "testCredential",
      "credentialValue" -> "secretCredential"))

    val pipelineContext = PipelineContext(Some(globalParameters), pipelineParameters, None, PipelineStepMapper(), None, List(),
      PipelineManager(List(subPipeline)), Some(credentialProvider), new ContextManager(Map(), Map()),
      stepResults, Some(PipelineStateKey("pipeline-id-3")))

    it("should pull the appropriate value for given parameters") {
      val tests = List(
        ("basic string concatenation missing value", Parameter(value = Some("!{bad-value}::concat_value")), "None::concat_value"),
        ("escaped template string", Parameter(value = Some("concat_value::\\!{globalBoolean}::!{pipelineId}::\\@{step-value}")), "concat_value::!{globalBoolean}::pipeline-id-3::@{step-value}"),
        ("escaped and unescaped template string", Parameter(value = Some("concat_value::\\!{pipelineId}::!{pipelineId}::\\@{step-value}")), "concat_value::!{pipelineId}::pipeline-id-3::@{step-value}"),
        ("basic string concatenation with multiple options around value",
          Parameter(value = Some(Some("!{tripleOption}::concat_value"))), "stringWithinAnOption::concat_value"),
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
        ("default value", Parameter(name = Some("fred"), defaultValue = Some("default value"), `type` = Some("string")), "default value"),
        ("string from global", Parameter(value = Some("!globalString"), `type` = Some("string")), "globalValue1"),
        ("GlobalLink overrides root global", Parameter(value = Some("!link9"), `type` = Some("string")), "link_override"),
        ("boolean from global", Parameter(value = Some("!globalBoolean"), `type` = Some("boolean")), true),
        ("integer from global", Parameter(value = Some("!globalInteger"), `type` = Some("integer")), Constants.FIVE),
        ("test object from global", Parameter(value = Some("!globalTestObject"), `type` = Some("text")), globalTestObject),
        ("child object from global", Parameter(value = Some("!globalTestObject.intField"), `type` = Some("integer")), globalTestObject.intField),
        ("non-step value from pipeline", Parameter(value = Some("$pipeline-id-1.rawKey1"), `type` = Some("string")), "rawValue1"),
        ("integer from current pipeline", Parameter(value = Some("$rawInteger"), `type` = Some("integer")), 3),
        ("missing global parameter", Parameter(value = Some("!fred"), `type` = Some("string")), None),
        ("missing runtime parameter", Parameter(value = Some("$fred"), `type` = Some("string")), None),
        ("None runtime parameter", Parameter(name = Some("red test"), value = Some("$pipeline-id-1.red"), `type` = Some("string")), None),
        ("integer from specific pipeline", Parameter(value = Some("$pipeline-id-2.rawInteger"), `type` = Some("integer")), 2),
        ("decimal from specific pipeline", Parameter(value = Some("$pipeline-id-2.rawDecimal"), `type` = Some("decimal")), 15.65),
        ("primary from current pipeline using @", Parameter(value = Some("@step1"), `type` = Some("text")), List(1, 2, 3)),
        ("primary from current pipeline using @ in GlobalLink", Parameter(value = Some("!link2"), `type` = Some("text")), List(1, 2, 3)),
//        ("primary from current pipeline using $ in GlobalLink", Parameter(value = Some("!link3"), `type` = Some("text")), List(1, 2, 3)),
        ("primary from specific pipeline using @", Parameter(value = Some("@pipeline-id-1.step1.primaryKey1String"), `type` = Some("string")),
          "primaryKey1Value"),
        ("primary from specific pipeline using @ in GlobalLink", Parameter(value = Some("!link4"), `type` = Some("string")), "primaryKey1Value"),
//        ("primary from specific pipeline using $", Parameter(value = Some("$pipeline-id-1.step1.primaryReturn.primaryKey1String"), `type` = Some("string")),
//          "primaryKey1Value"),
        ("primary from specific pipeline using $ in GlobalLink", Parameter(value = Some("!link5"), `type` = Some("string")), "primaryKey1Value"),
//        ("namedReturns from specific pipeline using $", Parameter(value = Some("$pipeline-id-1.step2.namedReturns.namedKey2String"), `type` = Some("string")),
//          "namedKey2Value"),
        ("namedReturns from specific pipeline using # in GlobalLink", Parameter(value = Some("!link6"), `type` = Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using #", Parameter(value = Some("#pipeline-id-1.step2.namedKey2String"), `type` = Some("string")),
          "namedKey2Value"),
        ("namedReturns from specific pipeline using #", Parameter(value = Some("!link7"), `type` = Some("string")), "namedKey2Value"),
        ("namedReturns from specific pipeline using # to be None", Parameter(value = Some("#pipeline-id-1.step2.nothing"), `type` = Some("string")), None),
        ("namedReturns from current pipeline using #", Parameter(value = Some("#step1.namedKey"), `type` = Some("string")), "namedValue"),
        ("resolve case class", Parameter(value = Some(classMap), className = Some("com.acxiom.metalus.ParameterTest")), ParameterTest(Some("fred"), Some(3))),
        ("resolve map", Parameter(value = Some(classMap)), classMap),
        ("resolve pipeline", Parameter(value = Some("&mypipeline")), subPipeline),
        ("fail to resolve pipeline", Parameter(value = Some("&mypipeline1")), None),
        ("fail to get global string", Parameter(value = Some("!invalidGlobalString")), None),
        ("fail to detect null", Parameter(value = Some("$nullValue || default string")), "default string"),
        ("recursive test", Parameter(value = Some("?pipeline-id-1.recursiveTest")), "rawValue1"),
        ("recursive test", Parameter(value = Some("$pipeline-id-1.recursiveTest")), "$pipeline-id-1.rawKey1"),
        ("lastStepId test", Parameter(value = Some("@LastStepId"), `type` = Some("text")), List(1, 2, 3)),
        ("lastStepId with or", Parameter(value = Some("!not_here || @LastStepId"), `type` = Some("text")), List(1, 2, 3)),
        ("lastStepId with method extraction", Parameter(value = Some("@LastStepId.nonEmpty"), `type` = Some("boolean")), true),
        ("inline list in inline map",
          Parameter(value = Some(Map[String, Any]("list" -> List("!globalString"))), `type` = Some("text")), Map("list" -> List("globalValue1"))),
        ("script to map a list of step returns",
          Parameter(value = Some(
            """(list: !complexList :Option[List[com.acxiom.metalus.PipelineStepResponse]])
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
            """(s:cChickens\:Rule\\:String)(s.toLowerCase.drop(1))"""), `type` = Some("scalascript")), "chickens:rule\\"),
        ("script with comma in type",
          Parameter(value = Some(
            """(pipelines:!pipelinesMap:List[Map[String,Any]]) pipelines.exists(p => p("pipelineId").asInstanceOf[String] == "30273bbb-983b-43ad-a549-6d23f3c0d143")"""),
            `type` = Some("scalascript")), true),
        ("Casting String global to int", Parameter(value = Some("!numericString"), `type` = Some("int")), 1),
        ("Casting String global to bigInt", Parameter(value = Some("!numericString"), `type` = Some("bigInt")), BigInt(1)),
        ("Casting String global to double", Parameter(value = Some("!numericString"), `type` = Some("double")), 1.0D),
        ("Casting BigInt global to long", Parameter(value = Some("!bigIntGlobal"), `type` = Some("long")), 1L),
        ("Casting BigInt global to BigDecimal", Parameter(value = Some("!bigIntGlobal"), `type` = Some("BigDecimal")), BigDecimal(1)),
        ("Casting bigInt global to String", Parameter(value = Some("!bigIntGlobal"), `type` = Some("String")), "1"),
        ("Casting String global to boolean", Parameter(value = Some("!booleanString"), `type` = Some("boolean")), true),
        ("Global String with runtime character", Parameter(value = Some("!runtimeGlobal"), `type` = Some("text")), "$rawKey1"),
        ("Use default value", Parameter(name = Some("defaultparam"), defaultValue = Some("default chosen"), `type` = Some("text")), "default chosen"),
        ("Pull Credential Name", Parameter(value = Some("%testCredential.name"), `type` = Some("text")), "testCredential"),
        ("Pull Credential Value", Parameter(value = Some("%testCredential.value"), `type` = Some("text")), "secretCredential"),
        ("Pull Escaped Credential Value", Parameter(value = Some("\\%testCredential%"), `type` = Some("text")), "%testCredential%")
      )
      tests.foreach(test => {
        Then(s"test ${test._1}")
        val ret = pipelineContext.parameterMapper.mapParameter(test._2, pipelineContext)
        assert(ret == test._3, test._1)
      })

      val thrown = intercept[RuntimeException] {
        pipelineContext.parameterMapper.mapParameter(Parameter(name = Some("badValue"), value=Some(1L),`type`=Some("long")), pipelineContext)
      }
      assert(Option(thrown).isDefined)
      val msg = "Unsupported value type class java.lang.Long for badValue!"
      assert(thrown.getMessage == msg)
      assert(pipelineContext.parameterMapper.mapParameter(
        Parameter(name = Some("badValue"), value=None,`type`=Some("string")), pipelineContext).asInstanceOf[Option[_]].isEmpty)

      assert(pipelineContext.parameterMapper.mapByType(None, Parameter(name = Some("defaultparam"),
        defaultValue = Some("default chosen"), `type` = Some("text")), pipelineContext) == "default chosen")
    }

    it("should cast string values to different numeric types") {
      Map(
        "int" -> 1,
        "integer" -> 1,
        "long" -> 1L,
        "float" -> 1.0F,
        "double" -> 1.0D,
        "byte" -> 1.toByte,
        "short" -> 1.toShort,
        "bigint" -> BigInt(1),
        "bigdecimal" -> BigDecimal(1)
      ).foreach{ case (typeName, expected) =>
        val ret = pipelineContext.parameterMapper.mapParameter(Parameter(value = Some("!numericString"), `type` = Some(typeName)), pipelineContext)
        assert(ret == expected)
      }
    }

    it("should cast bigInt values to different numeric and string types") {
      val globals = List("!int", "!long", "!float", "!double", "!byte", "!short", "!char", "!bigdecimal", "!bigIntGlobal")
      val casts = Map[String, Any => Boolean](
        "int" -> (a => a.isInstanceOf[Int]),
        "integer" -> (a => a.isInstanceOf[Int]),
        "long" -> (a => a.isInstanceOf[Long]),
        "float" -> (a => a.isInstanceOf[Float]),
        "double" -> (a => a.isInstanceOf[Double]),
        "byte" -> (a => a.isInstanceOf[Byte]),
        "short" -> (a => a.isInstanceOf[Short]),
        "character" -> (a => a.isInstanceOf[Char]),
        "char" -> (a => a.isInstanceOf[Char]),
        "boolean" -> (a => a.isInstanceOf[Boolean]),
        "bigint" -> (a => a.isInstanceOf[BigInt]),
        "bigdecimal" -> (a => a.isInstanceOf[BigDecimal]),
        "string" -> (a => a.isInstanceOf[String])
      )
      globals.flatMap(g => casts.map(p => (g, p._1, p._2)))
        .foreach{ case (global, typeName, expected) =>
        val ret = pipelineContext.parameterMapper.mapParameter(Parameter(value = Some(global), `type` = Some(typeName)), pipelineContext)
        assert(expected(ret), s"Reason: $global did not cast to $typeName")
      }
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
            Parameter(name = Some("One"), value=Some(classMap), className = Some("com.acxiom.metalus.ParameterTest")),
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
      val objectParameter = Parameter(value=Some(objectMap), className = Some("com.acxiom.metalus.ParameterTest"))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      assert(parameterValue == ParameterTest(Some("globalValue1"), Some(Constants.FIVE)))

      val ctx = pipelineContext.setGlobal("pipelineId", "pipeline-id-1")
        .setCurrentStateInfo(PipelineStateKey("pipeline-id-1"))
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
          className = Some("com.acxiom.metalus.NestedCaseClassTest")), ctx)
      assert(nestedCaseClass == NestedCaseClassTest(ParameterTest(Some("globalValue1"), Some(Constants.FIVE)),
        TestObject(2, "namedValue1", boolField = true, Map("globalTestKey1" -> "rawValue1"))))
    }

    it("Should replace variables in a map") {
      val objectMap = Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger", "concatString" -> "global->!{globalString}->value")
      val objectParameter = Parameter(value=Some(objectMap))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      assert(parameterValue.asInstanceOf[Map[String, Any]]("string").asInstanceOf[Option[String]].contains("globalValue1"))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("num").asInstanceOf[Option[Int]].contains(Constants.FIVE))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("concatString").asInstanceOf[Option[String]].contains("global->globalValue1->value"))
    }

    it("Should create a list of objects") {
      val objects = List(Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger"),
        Map[String, Any]("string" -> "secondObject", "num" -> (Constants.TEN)))
      val objectParameter = Parameter(value=Some(objects), className = Some("com.acxiom.metalus.ParameterTest"))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[ParameterTest]]
      assert(list.length == 2)
      assert(list.head == ParameterTest(Some("globalValue1"), Some(Constants.FIVE)))
      assert(list(1) == ParameterTest(Some("secondObject"), Some(Constants.TEN)))
    }

    it("Should create a list of maps") {
      val objects = List(Map[String, Any]("string" -> "!globalString", "num" -> "!globalInteger"),
        Map[String, Any]("string" -> "secondObject", "num" -> (Constants.TEN)))
      val objectParameter = Parameter(value=Some(objects))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Map[String, Any]]]
      assert(list.length == 2)
      assert(list.head("string").asInstanceOf[Option[String]].contains("globalValue1"))
      assert(list.head("num").asInstanceOf[Option[Int]].contains(Constants.FIVE))
      assert(list(1)("string").asInstanceOf[String] == "secondObject")
      assert(list(1)("num").asInstanceOf[Int] == (Constants.TEN))
    }

    it("Should return a list of items") {
      val objectParameter = Parameter(value=Some(List("string", Constants.FIVE, "global->!{globalString}->value")))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Any]]
      assert(list.length == 3)
      assert(list.head == "string")
      assert(list(1) == Constants.FIVE)
      assert(list(2) == "global->globalValue1->value")
    }

    it("Should respect the dropNoneFromLists option") {
      val objectParameter = Parameter(value=Some(List("string", Constants.FIVE, "!NotHere")))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      val list = parameterValue.asInstanceOf[List[Any]]
      assert(list.length == 2)
      assert(list.head == "string")
      assert(list(1) == Constants.FIVE)
      val contextWithOption = pipelineContext.setGlobal("dropNoneFromLists", false)
      val anotherValue = pipelineContext.parameterMapper.mapParameter(objectParameter, contextWithOption)
      val anotherList = anotherValue.asInstanceOf[List[Any]]
      assert(anotherList.length == 3)
      assert(anotherList.head == "string")
      assert(anotherList(1) == Constants.FIVE)
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
      val param = JsonParser.parseJson(json, "com.acxiom.metalus.Parameter").asInstanceOf[Parameter]
      val key = PipelineStateKey("test-pipeline")

      val ctx = params.foldLeft(pipelineContext)((context, entry) => {
        context.setPipelineParameterByKey(key, entry._1, entry._2)
      }).setCurrentStateInfo(key)
        .setGlobal("pipelineId", "test-pipeline")
      assert(ctx.parameterMapper.mapParameter(param, ctx) == "default string")
      assert(ctx.parameterMapper.mapParameter(Parameter(value = Some("$string")), ctx) == "some string")
      assert(ctx.parameterMapper.mapParameter(Parameter(value = Some("$int")), ctx) == 1)
    }

    it("Should replace complex variables in a map") {
      val objectMap = Map[String, Any](
        "string" -> "!globalString",
        "num" -> "!globalInteger",
        "concatString" -> "global->!{globalString}->value",
        "embeddedMap" -> Map[String, Any](
          "embeddedBoolean" -> "$badValue || !globalBoolean",
          "stepResponse" -> "@step3",
          "mappedInteger" -> "?rawInteger",
          "runtimeInteger" -> "$rawInteger",
          "parameterTest" -> Map[String, Any](
            "className" -> "com.acxiom.metalus.ParameterTest",
            "object" -> Map[String, Any]("string" -> "fred1", "num" -> 4)
          ),
          "listMap" -> List(
            Map[String, Any](
            "className" -> "com.acxiom.metalus.ParameterTest",
            "object" -> Map[String, Any]("string" -> "fred2", "num" -> 5)
          ),
            Map[String, Any](
              "className" -> "com.acxiom.metalus.ParameterTest",
              "object" -> Map[String, Any]("string" -> "fred3", "num" -> 6)
            ))
        ),
      "embeddedObject" -> Map[String, Any](
        "stepResponse" -> "@step3",
        "embeddedTest" -> Map[String, Any](
          "className" -> "com.acxiom.metalus.ParameterTest",
          "object" -> Map[String, Any]("string" -> "fred4", "num" -> 7)
        )
      ))
      val objectParameter = Parameter(value=Some(objectMap))
      val parameterValue = pipelineContext.parameterMapper.mapParameter(objectParameter, pipelineContext)
      assert(parameterValue.asInstanceOf[Map[String, Any]]("string").asInstanceOf[Option[String]].contains("globalValue1"))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("num").asInstanceOf[Option[Int]].contains(Constants.FIVE))
      assert(parameterValue.asInstanceOf[Map[String, Any]]("concatString").asInstanceOf[Option[String]].contains("global->globalValue1->value"))

      assert(parameterValue.asInstanceOf[Map[String, Any]]("embeddedMap").isInstanceOf[Map[String, Any]])
      val embeddedMap = parameterValue.asInstanceOf[Map[String, Any]]("embeddedMap").asInstanceOf[Map[String, Any]]
      assert(embeddedMap("embeddedBoolean").asInstanceOf[Option[Boolean]].contains(true))
      assert(embeddedMap("stepResponse").asInstanceOf[Option[String]].contains("fred_on_the_head"))
      assert(embeddedMap("mappedInteger").asInstanceOf[Option[Int]].contains(3))
      assert(embeddedMap("runtimeInteger").asInstanceOf[Option[Int]].contains(3))

      assert(embeddedMap("parameterTest").isInstanceOf[ParameterTest])
      val parameterTest = embeddedMap("parameterTest").asInstanceOf[ParameterTest]
      assert(parameterTest == ParameterTest(Some("fred1"), Some(Constants.FOUR)))

      assert(embeddedMap("listMap").isInstanceOf[List[ParameterTest]])
      val listTest = embeddedMap("listMap").asInstanceOf[List[ParameterTest]]
      assert(listTest.length == 2)
      assert(listTest.head == ParameterTest(Some("fred2"), Some(Constants.FIVE)))
      assert(listTest(1) == ParameterTest(Some("fred3"), Some(Constants.SIX)))

      assert(parameterValue.asInstanceOf[Map[String, Any]]("embeddedObject").isInstanceOf[Map[String, Any]])
      val embeddedObject = parameterValue.asInstanceOf[Map[String, Any]]("embeddedObject").asInstanceOf[Map[String, Any]]
      assert(embeddedObject("stepResponse").asInstanceOf[Option[String]].contains("fred_on_the_head"))
      assert(embeddedObject("embeddedTest").isInstanceOf[ParameterTest])
      val embeddedTest = embeddedObject("embeddedTest").asInstanceOf[ParameterTest]
      assert(embeddedTest == ParameterTest(Some("fred4"), Some(Constants.SEVEN)))
    }
  }
}

case class NestedCaseClassTest(parameterTest: ParameterTest, testObject: TestObject)
case class ParameterTest(string: Option[String], num: Option[Int])
case class TestObject(intField: Integer, stringField: String, boolField: Boolean, mapField: Map[String, Any])
