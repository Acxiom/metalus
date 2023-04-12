package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineStateKey, PipelineStepMapper}
import com.acxiom.metalus.connectors.InMemoryDataConnector
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.sql.{DataFrame, DataReference, Row}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.funspec.AnyFunSpec

class SqlParserTests extends AnyFunSpec {

  implicit val format: Formats = DefaultFormats

  lazy val pipelineContext: PipelineContext = {
    val chickensData = List(
      Row(Array(1, "Cogburn", "GameCock", true), None, None),
      Row(Array(2, "Goldhen", "Sex-link", false), None, None),
      Row(Array(3, "Blackhen", "Sex-link", false), None, None),
      Row(Array("4".toInt, "Honey", "Orpington", false), None, None)
    )
    val ctx = PipelineContext(None,
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
    val dr = InMemoryDataConnector("test").fromSeq(chickensData, None, ctx)
    ctx.setGlobal("chickens", dr)
  }

  describe("SqlParser - Basic") {
    it("should parse a select statement into a pipeline") {
      val text =
        """SELECT c.name, b.name as breed FROM @chickens c
          |JOIN @breeds b on c.breed_id = b.id
          |""".stripMargin
      val steps = SqlParser.parseToSteps(text)
      assert(steps.nonEmpty)
      assert(steps.last.id.mkString == "SELECT_c_b")
    }

    it("should parse an update statement into a pipeline") {
      val text =
        """UPDATE @chickens
          |SET breed_id = 1
          |WHERE name = 'cogburn'
          |""".stripMargin
      val steps = SqlParser.parseToSteps(text)
      assert(steps.nonEmpty)
    }
  }

  describe("DataReference Parser - Basic") {
    it("should build a data reference from sql") {
      val res = DataReference.sql("SELECT * FROM !chickens WHERE col_2 = 'Sex-link'", pipelineContext)
        .execute.asInstanceOf[DataFrame[_]]
      assert(res.count() == 2)
      assert(res.collect().map(_.apply("col_1")).toList == List("Goldhen", "Blackhen"))
    }
  }

}
