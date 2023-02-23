package com.acxiom.metalus.steps

import com.acxiom.metalus.ClassInfo
import com.acxiom.metalus.applications.Json4sSerializers
import com.acxiom.metalus.steps.Color.Color
import org.json4s.JsonAST.JString
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.JsonMethods
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JObject}
import org.scalatest.funspec.AnyFunSpec

class JSONStepsTests extends AnyFunSpec {
  private val jsonString =
    """{
      | "property1": "abc",
      | "property2": {
      |   "subproperty1": "def",
      |   "subproperty2": 1
      | }
      |}""".stripMargin

  describe("JSONSteps - Conversion Steps") {
    it("Should convert a JSON string to a map") {
      val map = JSONSteps.jsonStringToMap(jsonString, None)
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
      val string = JSONSteps.jsonMapToString(map, None)
      assert(Option(string).isDefined)
      assert(string.nonEmpty)
      assert(string.compareTo(jsonString.replaceAll("\\s+", "")) == 0)
    }

    it("Should convert an object to a JSON string") {
      val obj = TestObject("abc", SubTestObject("def", 1))
      val string = JSONSteps.objectToJsonString(obj, None)
      assert(Option(string).isDefined)
      assert(string.nonEmpty)
      assert(string.compareTo(jsonString.replaceAll("\\s+", "")) == 0)
    }

    it("Should convert a JSON string to an object") {
      val json =
        """
          |{
          |   "property1": "moo",
          |   "property2": {
          |      "subproperty1": "moo2",
          |      "subproperty2": 1
          |   }
          |}
          |""".stripMargin
      val res = JSONSteps.jsonStringToObject(json, "com.acxiom.metalus.steps.TestObject", None)
      assert(res.isInstanceOf[TestObject])
      val test = res.asInstanceOf[TestObject]
      assert(test.property1 == "moo")
      assert(test.property2.subproperty1 == "moo2")
      assert(test.property2.subproperty2 == 1)
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
          |     },
          |     "nullable": false
          |   },
          |   {
          |     "name": "col2",
          |     "dataType": {
          |       "baseType": "integer"
          |     },
          |     "metadata": {"key": "value"},
          |     "nullable": true
          |   },
          |   {
          |     "name": "col3",
          |     "dataType": {
          |       "baseType": "decimal"
          |     }
          |   }
          | ]
          |}""".stripMargin
      val schema = JSONSteps.jsonStringToSchema(string, None)
      assert(Option(schema).isDefined)
      assert(schema.attributes.length == 3)
      assert(schema.attributes.head.name == "col1")
      assert(schema.attributes.head.dataType.baseType == "string")
      assert(!schema.attributes.head.nullable.getOrElse(true))
      assert(schema.attributes(1).name == "col2")
      assert(schema.attributes(1).dataType.baseType == "integer")
      assert(schema.attributes(1).nullable.getOrElse(true))
      assert(schema.attributes(1).metadata.getOrElse(Map()) == Map("key" -> "value"))
      assert(schema.attributes(2).name == "col3")
      assert(schema.attributes(2).dataType.baseType == "decimal")
    }
  }

  // TODO [2.0 Review] This needs to be updated to DataReference
//  describe("JSONSteps - DataFrame") {
//    it("Should convert single line JSON data to a DataFrame") {
//      validateDataFrame(
//        """{"col1": "row1_col1", "col2": "row1_col2", "col3": "row1_col3"}
//           {"col1": "row2_col1", "col2": "row2_col2", "col3": "row2_col3"}
//           {"col1": "row3_col1", "col2": "row3_col2", "col3": "row3_col3"}""")
//    }
//
//    it("Should convert Multiline JSON data to a DataFrame") {
//      validateDataFrame(
//        """[{ "col1": "row1_col1", "col2": "row1_col2", "col3": "row1_col3" },
//          |{ "col1": "row2_col1", "col2": "row2_col2", "col3": "row2_col3" },
//          |{ "col1": "row3_col1", "col2": "row3_col2", "col3": "row3_col3" }]""".stripMargin)
//    }
//
//
//    it("Should convert a Dataset[String] of JSON strings to a DataFrame") {
//      val spark = sparkSession
//      val json = Seq(
//        """{"id":1, "name": "silkie", "stats": {"toes": 5, "skin_color": "black", "comb": "walnut"}}""",
//        """{"id":2, "name":"leghorn", "stats": {"toes": 4, "skin_color": "yellow", "comb": "single"}}""",
//        """{"id":3, "name": "polish", "stats": {"toes": 4, "skin_color": "white", "comb": "v-comb"}}"""
//      ).toDS
//      val df = JSONSteps.jsonDatasetToDataFrame(json, None, pipelineContext)
//      assert(df.count == 3)
//      assert(df.columns.length == 3)
//      assert(df.where("stats.toes = 5").selectExpr("name").first.getString(0) == "silkie")
//    }
//
//    it("Should convert a Dataset[String] of JSON strings to a DataFrame and respect options") {
//      val spark = sparkSession
//      val json = Seq(
//        """{"id":1, "name": "silkie", "stats": {"toes": 5, "skin_color": "black", "comb": "walnut"}}""",
//        """{"id":2, "name":"leghorn", "stats": {"toes": 4, "skin_color": "yellow", "comb": "single"}}""",
//        """{"id":3, "name": "polish", "stats": {"toes": 4, "skin_color": "white", "comb": "v-comb"}}"""
//      ).toDS
//      val dataFrameReaderOptions = DataFrameReaderOptions("json", Some(Map("primitivesAsString" -> "true")))
//      val df = JSONSteps.jsonDatasetToDataFrame(json, Some(dataFrameReaderOptions), pipelineContext)
//      assert(df.count == 3)
//      assert(df.columns.length == 3)
//      assert(df.schema.fields.exists(f => f.name == "id" && f.dataType == StringType))
//      assert(df.where("stats.toes = '5'").selectExpr("name").first.getString(0) == "silkie")
//    }
//  }

  describe("JSONSteps - Formats") {
    it("Should build json4s Formats") {
      val formats = Json4sSerializers(
        Some(List(ClassInfo(Some("com.acxiom.metalus.steps.ChickenSerializer")))),
        None,
        Some(List(ClassInfo(Some("com.acxiom.metalus.steps.Color"))))
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
      val obj = JSONSteps.jsonStringToObject(json, "com.acxiom.metalus.steps.Coop", Some(formats))
      assert(obj.isInstanceOf[Coop])
      val coop = obj.asInstanceOf[Coop]
      assert(coop.name == "chicken-coop")
      assert(coop.color == Color.WHITE)
      assert(coop.chickens.exists(c => c.breed == "silkie" && c.color == Color.BUFF))
      assert(coop.chickens.exists(c => c.breed == "polish" && c.color == Color.BLACK))
    }

    it("should build a formats object with only default formats if no custom serializers are provided") {
      val formats = Json4sSerializers()
      assert(formats.customSerializers.isEmpty)
      val res = JSONSteps.jsonStringToMap("""{"k1":"v1","k2":"v2"}""", Some(formats))
      assert(res.isDefined)
      val k1 = res.get.get("k1")
      val k2 = res.get.get("k2")
      assert(k1.isDefined && k1.get.asInstanceOf[String] == "v1")
      assert(k2.isDefined && k2.get.asInstanceOf[String] == "v2")
    }
  }

//  private def validateDataFrame(jsonString: String) = {
//    val df = JSONSteps.jsonStringToDataFrame(jsonString, PipelineContext(sparkConf = Some(sparkConf),
//      sparkSession = Some(sparkSession), globals = None, parameters = PipelineParameters(), stepMessages = None))
//    assert(df.count() == 3)
//    assert(df.schema.fields.length == 3)
//    assert(df.schema.fields.head.name == "col1")
//    assert(df.schema.fields(1).name == "col2")
//    assert(df.schema.fields(2).name == "col3")
//    val list = df.collect()
//    assert(list.head.getString(0) == "row1_col1")
//    assert(list.head.getString(1) == "row1_col2")
//    assert(list.head.getString(2) == "row1_col3")
//    assert(list(1).getString(0) == "row2_col1")
//    assert(list(1).getString(1) == "row2_col2")
//    assert(list(1).getString(2) == "row2_col3")
//    assert(list(2).getString(0) == "row3_col1")
//    assert(list(2).getString(1) == "row3_col2")
//    assert(list(2).getString(2) == "row3_col3")
//  }
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

class ChickenSerializer extends CustomSerializer[Chicken](_ => ( {
  case json: JObject =>
    implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(Color) + new ChickenSerializer
    if((json \ "breed") == JString("silkie")) {
      json.extract[Silkie]
    } else {
      json.extract[Polish]
    }
}, {
  case chicken: Chicken =>
    JsonMethods.parse(s"""{"breed":"${chicken.breed},"color":"${chicken.color.toString}"}""")
}))
