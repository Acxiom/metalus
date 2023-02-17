package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.{DefaultPipelineListener, Parameter, PipelineContext, PipelineParameter, PipelineStateInfo, PipelineStepMapper}
import org.scalatest.funspec.AnyFunSpec

class ExpressionParserTests extends AnyFunSpec {

  lazy val pipelineContext: PipelineContext = {
    PipelineContext(Some(Map[String, Any](
      "chicken" -> "silkie",
      "bird" -> "chicken",
      "nested" -> Map("chicken" -> Some("gamecock"))
    )),
      List(PipelineParameter(PipelineStateInfo("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateInfo("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
  }

  implicit val ke: ExpressionParser.KeywordExecutor = {
    case ExpressionParser.VALUE => Some("polish_chickens")
    case ExpressionParser.STEP => Some("leghorn_chickens")
  }

  describe("ExpressionParser - Basic") {

    val basicTests = List(
      // basic
      ("'silkie'", "silkie"),
      ("'silkie' + '_chicken'", "silkie_chicken"),
      ("!chicken + '_chicken'", "silkie_chicken"),
      ("!chicken || !bird", "silkie"),
      ("!bad || !bird", "chicken"),
      ("!nested.chicken + '_chicken'", "gamecock_chicken"),
      // keywords
      ("VALUE", "polish_chickens"),
      ("STEP", "leghorn_chickens"),
      // boolean
      ("TRUE", true),
      ("FALSE", false),
      ("!chicken AND !bird", true),
      ("!bad AND !bird", false),
      ("!bad OR !chicken", true),
      ("NOT !bad", true),
      // complex
      ("IF ((!chicken + '_' + !bird) != 'silkie_chicken') 'regular' ELSE 'bantam'", "bantam")
    )

    it("should evaluate basic expressions") {
      val parseTest = ExpressionParser.parse(_, pipelineContext)
      val start = System.nanoTime()
      basicTests.foreach{ case (test, expected) =>
        assert(parseTest(test).contains(expected), test)
      }
      val end = System.nanoTime() - start
      println(end)
    }

    it("should throw a parse exception on illegal syntax") {
      val exe = intercept[ParseException] {
        ExpressionParser.parse("<BAD SYNTAX>", pipelineContext)
      }
      assert(exe.message.startsWith("mismatched input '<' expecting"))
    }
  }

}
