package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.InMemoryDataConnector
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineStateKey, PipelineStepMapper}
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
      Map("id" -> 1, "name" -> "Cogburn", "breed" -> "GameCock", "rooster" -> true),
      Map("id" -> 2, "name" -> "Goldhen", "breed" -> "Sex-link", "rooster" -> false),
      Map("id" -> 3, "name" -> "Blackhen", "breed" -> "Sex-link", "rooster" -> false),
      Map("id" -> 4, "name" -> "Honey", "breed" -> "Orpington", "rooster" -> false)
    )
    val csvData = "id,name,breed,rooster\n" + chickensData.map(r => r.values.mkString(",")).mkString("\n")
    val conn = InMemoryDataConnector("test")
    val chickens = conn.createDataReference(Some(Map("data" -> chickensData)), pipelineContext)

    it("should query an in memory data reference") {
      val res = chickens + Where(Expression("NOT rooster")) +
        Select(List(Expression("name AS chicken_name"), Expression("rooster AS is_rooster"), Expression("1 as `int`")))
      val table = res.asInstanceOf[InMemoryDataReference].execute
      assert(table.data.length == 3)
      assert(table.schema.attributes.exists(_.name == "chicken_name"))
      assert(table.schema.attributes.exists(a => a.name == "is_rooster" && a.dataType.baseType == "Boolean"))
      assert(table.schema.attributes.exists(a => a.name == "int" && a.dataType.baseType == "BigInt"))
    }

    it ("should group an in memory data reference") {
      val res = chickens + GroupBy(List(Expression("breed"))) +
        Select(List(Expression("breed AS breed_name"), Expression("COUNT(id) AS breed_count"))) +
        Where(Expression("breed_count = 2"))
      val table = res.asInstanceOf[InMemoryDataReference].execute
      assert(table.data.size == 1)
      assert(table.schema.attributes.exists(_.name == "breed_name"))
      assert(table.schema.attributes.exists(a => a.name == "breed_count" && a.dataType.baseType == "BigInt"))
      assert(table.data.head(0).toString == "Sex-link")
    }

    it("should handle a csv") {
      val stream = new ByteArrayInputStream(csvData.getBytes)
      val csvChickens =
        conn.createDataReference(Some(Map("data" -> stream, "inferDataTypes" -> "true")), pipelineContext) +
          Where(Expression("rooster")) +
        Select(List(Expression("name AS chicken_name"), Expression("rooster AS is_rooster"), Expression("1 as `int`")))
      val table = csvChickens.asInstanceOf[InMemoryDataReference].execute
      assert(table.data.length == 1)
      assert(table.schema.attributes.exists(_.name == "chicken_name"))
      assert(table.schema.attributes.exists(a => a.name == "is_rooster" && a.dataType.baseType == "Boolean"))
      assert(table.schema.attributes.exists(a => a.name == "int" && a.dataType.baseType == "BigInt"))
      assert(table.data.head(0).toString == "Cogburn")
    }
  }

  def getRow(id: Int, rooster: Boolean): Map[String, Any] =
    Map("id" -> id, "name" -> "Honey", "breed" -> (if (rooster) "GameCock" else "Orpington"), "rooster" -> rooster)
}
