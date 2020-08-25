package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import com.acxiom.pipeline.applications.ClassInfo
import com.acxiom.pipeline.steps.Color.Color
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JObject}
import org.json4s.JsonAST.JString
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.JsonMethods.parse
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class JSONStepsTests extends FunSpec with BeforeAndAfterAll {
  private val MASTER = "local[2]"
  private val APPNAME = "json-steps-spark"
  private var sparkConf: SparkConf = _
  private var sparkSession: SparkSession = _
  private val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  private var pipelineContext: PipelineContext = _

  private val jsonString =
    """{
      | "property1": "abc",
      | "property2": {
      |   "subproperty1": "def",
      |   "subproperty2": 1
      | }
      |}""".stripMargin

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

  describe("JSONSteps - Conversion Steps") {
    it("Should convert a JSON string to a map") {
      val map = JSONSteps.jsonStringToMap(jsonString, None, pipelineContext)
      assert(map.isDefined)
      assert(map.get.keySet.size == 2)
      assert(map.get.contains("property1"))
      assert(map.get("property1") == "abc")
      assert(map.get.contains("property2"))
      val subMap = map.get("property2").asInstanceOf[Map[String, Any]]
      assert(subMap.keySet.size == 2)
      assert(subMap.contains("subproperty1"))
      assert(subMap("subproperty1") == "def")
      assert(subMap.contains("subproperty2"))
      assert(subMap("subproperty2") == 1)
    }

    it("Should convert a map to a JSON string") {
      val map = Map[String, Any]("property1" -> "abc",
      "property2" -> Map[String, Any]("subproperty1" -> "def", "subproperty2" -> 1))
      val string = JSONSteps.jsonMapToString(map, None, pipelineContext)
      assert(Option(string).isDefined)
      assert(string.nonEmpty)
      assert(string.compareTo(jsonString.replaceAll("\\s+","")) == 0)
    }

    it("Should convert an object to a JSON string") {
      val obj = TestObject("abc", SubTestObject("def", 1))
      val string = JSONSteps.objectToJsonString(obj, None, pipelineContext)
      assert(Option(string).isDefined)
      assert(string.nonEmpty)
      assert(string.compareTo(jsonString.replaceAll("\\s+","")) == 0)
    }
  }

  describe("JSONSteps - Schema") {
    it("Should convert a JSON string to a Schema") {
      val string =
        """{
          | "attributes": [
          |   {
          |     "name": "col1",
          |     "dataType": {
          |       "baseType": "string"
          |     }
          |   },
          |   {
          |     "name": "col2",
          |     "dataType": {
          |       "baseType": "integer"
          |     }
          |   },
          |   {
          |     "name": "col3",
          |     "dataType": {
          |       "baseType": "decimal"
          |     }
          |   }
          | ]
          |}""".stripMargin
      val schema = JSONSteps.jsonStringToSchema(string, None, pipelineContext)
      assert(Option(schema).isDefined)
      assert(schema.attributes.length == 3)
      assert(schema.attributes.head.name == "col1")
      assert(schema.attributes.head.dataType.baseType == "string")
      assert(schema.attributes(1).name == "col2")
      assert(schema.attributes(1).dataType.baseType == "integer")
      assert(schema.attributes(2).name == "col3")
      assert(schema.attributes(2).dataType.baseType == "decimal")
    }
  }

  describe("JSONSteps - DataFrame") {
    it("Should convert single line JSON data to a DataFrame") {
        validateDataFrame("""{"col1": "row1_col1", "col2": "row1_col2", "col3": "row1_col3"}
           {"col1": "row2_col1", "col2": "row2_col2", "col3": "row2_col3"}
           {"col1": "row3_col1", "col2": "row3_col2", "col3": "row3_col3"}""")
    }

    it("Should convert Multiline JSON data to a DataFrame") {
        validateDataFrame("""[{ "col1": "row1_col1", "col2": "row1_col2", "col3": "row1_col3" },
          |{ "col1": "row2_col1", "col2": "row2_col2", "col3": "row2_col3" },
          |{ "col1": "row3_col1", "col2": "row3_col2", "col3": "row3_col3" }]""".stripMargin)
    }
  }

  describe("JSONSteps - Formats") {
    it("Should build json4s Formats") {
      val formats = JSONSteps.buildJsonFormats(
        Some(List(ClassInfo(Some("com.acxiom.pipeline.steps.ChickenSerializer")))),
        None,
        Some(List(ClassInfo(Some("com.acxiom.pipeline.steps.Color"))))
      )
      val json =
        """{
          |   "name": "chicken-coop",
          |   "color": "WHITE",
          |   "chickens": [
          |      {
          |         "breed": "silkie",
          |         "color": "BUFF"
          |      },
          |      {
          |         "breed": "polish",
          |         "color": "BLACK"
          |      }
          |   ]
          |}""".stripMargin
      val obj = JSONSteps.jsonStringToObject(json, "com.acxiom.pipeline.steps.Coop", Some(formats), pipelineContext)
      assert(obj.isInstanceOf[Coop])
      val coop = obj.asInstanceOf[Coop]
      assert(coop.name == "chicken-coop")
      assert(coop.color == Color.WHITE)
      assert(coop.chickens.exists(c => c.breed == "silkie" && c.color == Color.BUFF))
      assert(coop.chickens.exists(c => c.breed == "polish" && c.color == Color.BLACK))
    }

    it("should be a formats object with only default formats if no custom serializers are provided") {
      val formats = JSONSteps.buildJsonFormats()
      assert(formats.customSerializers.isEmpty)
      val res = JSONSteps.jsonStringToMap("""{"k1":"v1","k2":"v2"}""", Some(formats), pipelineContext)
      assert(res.isDefined)
      val k1 = res.get.get("k1")
      val k2 = res.get.get("k2")
      assert(k1.isDefined && k1.get.asInstanceOf[String] == "v1")
      assert(k2.isDefined && k2.get.asInstanceOf[String] == "v2")
    }
  }

  private def validateDataFrame(jsonString: String) = {
    val df = JSONSteps.jsonStringToDataFrame(jsonString, PipelineContext(sparkConf = Some(sparkConf),
      sparkSession = Some(sparkSession), globals = None, parameters = PipelineParameters(), stepMessages = None))
    assert(df.count() == 3)
    assert(df.schema.fields.length == 3)
    assert(df.schema.fields.head.name == "col1")
    assert(df.schema.fields(1).name == "col2")
    assert(df.schema.fields(2).name == "col3")
    val list = df.collect()
    assert(list.head.getString(0) == "row1_col1")
    assert(list.head.getString(1) == "row1_col2")
    assert(list.head.getString(2) == "row1_col3")
    assert(list(1).getString(0) == "row2_col1")
    assert(list(1).getString(1) == "row2_col2")
    assert(list(1).getString(2) == "row2_col3")
    assert(list(2).getString(0) == "row3_col1")
    assert(list(2).getString(1) == "row3_col2")
    assert(list(2).getString(2) == "row3_col3")
  }
}

case class TestObject(property1: String, property2: SubTestObject)
case class SubTestObject(subproperty1: String, subproperty2: Int)

object Color extends Enumeration {
  type Color = Value
  val BLACK, GOLD, BUFF, WHITE = Value
}

trait Chicken {
  val breed: String
  val color: Color
}

case class Silkie(color: Color) extends Chicken {
  override val breed: String = "silkie"
}

case class Polish(color: Color) extends Chicken {
  override val breed: String = "polish"
}

case class Coop(name: String, chickens: List[Chicken], color: Color)

class ChickenSerializer extends CustomSerializer[Chicken](f => ( {
  case json: JObject =>
    implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(Color) + new ChickenSerializer
    if((json \ "breed") == JString("silkie")) {
      json.extract[Silkie]
    } else {
      json.extract[Polish]
    }
}, {
  case chicken: Chicken =>
    parse(s"""{"breed":"${chicken.breed},"color":"${chicken.color.toString}"}""")
}))
