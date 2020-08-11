package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline.{PipelineContext, PipelineParameters}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class JSONStepsTests extends FunSpec with BeforeAndAfterAll {
  private val MASTER = "local[2]"
  private val APPNAME = "data-frame-steps-spark"
  private var sparkConf: SparkConf = _
  private var sparkSession: SparkSession = _
  private val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")

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
      val map = JSONSteps.jsonStringToMap(jsonString)
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
      val string = JSONSteps.jsonMapToString(map)
      assert(Option(string).isDefined)
      assert(string.nonEmpty)
      assert(string.compareTo(jsonString.replaceAll("\\s+","")) == 0)
    }

    it("Should convert an object to a JSON string") {
      val obj = TestObject("abc", SubTestObject("def", 1))
      val string = JSONSteps.objectToJsonString(obj)
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
      val schema = JSONSteps.jsonStringToSchema(string)
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
