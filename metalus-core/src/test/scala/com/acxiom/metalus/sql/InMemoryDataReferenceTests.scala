package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.InMemoryDataConnector
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineStateKey, PipelineStepMapper}
import com.acxiom.metalus.sql.Expression._
import org.scalatest.funspec.AnyFunSpec

import java.io.ByteArrayInputStream

class InMemoryDataReferenceTests extends AnyFunSpec {

  lazy val pipelineContext: PipelineContext = {
    PipelineContext(Some(Map[String, Any]()),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
  }

  describe("InMemoryDataReference - Basic") {
    val chickensData = List(
      Row(Array(1, "Cogburn", "GameCock", true), None, None),
      Row(Array(2, "Goldhen", "Sex-link", false), None, None),
      Row(Array(3, "Blackhen", "Sex-link", false), None, None),
      Row(Array("4".toInt, "Honey", "Orpington", false), None, None)
    )
    val schema = Schema(List(
      Attribute("id", AttributeType("int"), Some(true), None),
      Attribute("name", AttributeType("string"), Some(true), None),
      Attribute("breed", AttributeType("string"), Some(true), None),
      Attribute("rooster", AttributeType("boolean"), Some(true), None)
    ))
    val csvData = "id,name,breed,rooster\n" + chickensData.map(r => r.columns.mkString(",")).mkString("\n")
    val conn = InMemoryDataConnector("test")
    val chickens = conn.createDataReference(Some(Map("data" -> chickensData, "schema" -> schema)), pipelineContext)

    it("should query an in memory data reference") {
      val res = chickens + Where(Expression("NOT rooster")) +
        Select(List(Expression("name AS chicken_name"), Expression("rooster AS is_rooster"), Expression("1 as `int`")))
      val table = res.asInstanceOf[InMemoryDataReference].execute
      assert(table.count() == 3)
      assert(table.schema.attributes.exists(_.name == "chicken_name"))
      assert(table.schema.attributes.exists(a => a.name == "is_rooster" && a.dataType.baseType == "BOOLEAN"))
      assert(table.schema.attributes.exists(a => a.name == "int" && a.dataType.baseType == "LONG"))
    }

    it ("should group an in memory data reference") {
      val res = chickens + GroupBy(List(Expression("breed"))) +
        Select(List(Expression("breed AS breed_name"), Expression("COUNT(id) AS breed_count"))) +
        Where(Expression("breed_count = 2"))
      val table = res.asInstanceOf[InMemoryDataReference].execute
      assert(table.count() == 1)
      assert(table.schema.attributes.exists(_.name == "breed_name"))
      assert(table.schema.attributes.exists(a => a.name == "breed_count" && a.dataType.baseType == "DOUBLE"))
      assert(table.collect().head(0).toString == "Sex-link")
    }

    it("should handle a csv") {
      val stream = new ByteArrayInputStream(csvData.getBytes)
      val csvChickens =
        conn.createDataReference(Some(Map("data" -> stream, "header" -> "true")), pipelineContext) +
          Where(col("rooster")) +
        Select(List(col("name").as("chicken_name"), col("rooster").as("is_rooster"),
          lit("1").cast("int").as("`int`")))
      val table = csvChickens.asInstanceOf[InMemoryDataReference].execute
      assert(table.count() == 1)
      assert(table.schema.attributes.exists(_.name == "chicken_name"))
      assert(table.schema.attributes.exists(a => a.name == "is_rooster" && a.dataType.baseType == "BOOLEAN"))
      assert(table.schema.attributes.exists(a => a.name == "int" && a.dataType.baseType == "INTEGER"))
      assert(table.collect().head(0).toString == "Cogburn")
    }

    it("should handle null fields") {
      val withNull = chickensData :+ Row(Array("5".toInt, "Pepper", None.orNull, None.orNull), None, None)
      val ref = conn.createDataReference(Some(Map("data" -> withNull, "schema" -> schema)), pipelineContext) +
        Where(expr("breed = null"))
      val df = ref.asInstanceOf[InMemoryDataReference].execute
      assert(df.count() == 1)
      val res = df.collect()
      assert(res.head(1) == "Pepper")
      assert(Option(res.head(3)).isEmpty)
    }
  }

  def getRow(id: Int, rooster: Boolean): Map[String, Any] =
    Map("id" -> id, "name" -> "Honey", "breed" -> (if (rooster) "GameCock" else "Orpington"), "rooster" -> rooster)
}
