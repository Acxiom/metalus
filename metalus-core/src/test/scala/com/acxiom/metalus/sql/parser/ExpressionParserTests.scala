package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.connectors.InMemoryDataConnector
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.sql.Row
import com.acxiom.metalus.utils.ReflectionUtils.getClass
import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineException, PipelineParameter, PipelineStateKey, PipelineStepMapper}
import org.scalatest.funspec.AnyFunSpec

class ExpressionParserTests extends AnyFunSpec {

  lazy val pipelineContext: PipelineContext = {
    val chickensData = List(
      Row(Array(1, "Cogburn", "GameCock", true), None, None),
      Row(Array(2, "Goldhen", "Sex-link", false), None, None),
      Row(Array(3, "Blackhen", "Sex-link", false), None, None),
      Row(Array("4".toInt, "Honey", "Orpington", false), None, None)
    )
    val ctx = PipelineContext(Some(Map[String, Any](
      "chicken" -> "silkie",
      "bird" -> "chicken",
      "nested" -> Map("chicken" -> Some("gamecock")),
      "four" -> 4,
      "long" -> 4L
    )),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
    val dr = InMemoryDataConnector("test").fromSeq(chickensData, None, ctx)
    ctx.setGlobal("chickens", dr)
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
      ("1 < 2", true),
      ("1 > 2", false),
      ("!four > 2", true),
      ("!four >= !four", true),
      ("3.6 <= !four", true),
      // arithmetic
      ("1 + 2", 3),
      ("!four % 2", 0),
      ("!four / 2", 2),
      ("!four - !four", 0),
      ("2 + 3.0 * 2", 8.0), // check precedence
      ("-!four", -4L),
      ("abs(-2.2)", 2.2),
      ("min(1, 0)", 0),
      ("max(5, 2.1)", 5),
      ("min(!four, !notHere)", 4),
      ("ceil(5.4)", 6.0),
      ("floor(5.6)", 5.0),
      ("round(5.4)", 5.0),
      // collections
      ("['1', '2', '3']", List("1", "2", "3")),
      ("[]", List()),
      ("{'a': 1, 'b': 2, 'c': 3}", Map("a" -> 1, "b" -> 2, "c" -> 3)),
      ("{}", Map.empty[String, Any]),
      ("['a', 'b', 'c'].reduce(p => p.left + p.right)", "abc"),
      ("{'a': 1, 'b': 2, 'c': 3}.map(p => p._1).reduce(p => p.left + p.right)", "abc"),
      ("{'a': 1, 'b': 2, 'c': 3}.map(p => p._1 + p._2).to_list", List("a1", "b2", "c3")),
      ("['a', 'b', 'c'].exists(p => p = 'a')", true),
      ("['a', 'b', 'c'].find(p => p = 'a')", "a"),
      ("{'a': 1, 'b': 2, 'c': 3}.to_list.last", ("c", 3)),
      ("(['a', 'b'] ++ ['c', 'd']).mkString", "abcd"),
      // complex
      ("IF ((!chicken + '_' + !bird) != 'silkie_chicken') 'regular' ELSE 'bantam'", "bantam"),
      ("!chickens.execute.collect[0]['col_1']", "Cogburn"),
      ("com.acxiom.metalus.sql.parser.JavaStyle('chickens', '!').mkString", "chickens,!"),
      ("com.acxiom.metalus.sql.parser.JavaStyle(Some(1)).mkString", "1,default"),
      ("com.acxiom.metalus.sql.parser.JavaStyle().mkString", "more,default"), // verify empty args work
      ("com.acxiom.metalus.sql.parser.ScalaStyle(!chicken + '!', !long - 1, Some('moo'), None).mkString", "silkie!,3,moo,o2,d")
    )

    it("should evaluate basic expressions") {
      val parseTest = ExpressionParser.parse(_, pipelineContext)
      basicTests.foreach{ case (test, expected) =>
         assert(parseTest(test).contains(expected), test)
      }
    }

    it("should evaluate a long expression chain") {
      val expr = (0 until 500).map(_ => "false").mkString(" OR ") + " OR TRUE"
      assert(ExpressionParser.parse(expr, pipelineContext).exists(_.toString == "true"))
    }

    it("should throw a parse exception on illegal syntax") {
      val exe = intercept[ParseException] {
        ExpressionParser.parse("<BAD SYNTAX>", pipelineContext)
      }
      assert(exe.message.startsWith("extraneous input '<' expecting"))
    }

    it("should throw an exception on bad syntax if strictExpressions is enabled") {
      val exe = intercept[PipelineException] {
        ExpressionParser.parse("bad.Object('moo')", pipelineContext.setGlobal("strictExpressions", true))
      }
      assert(exe.message.contains("Failed to instantiate object: bad.Object"))
    }
  }

}

class JavaStyle(s: String, s2: String) {
  def this(moo: Option[Int]) = this(moo.mkString, "default")
  def this() = this("more", "default")

  def mkString: String = s"$s,$s2"
}

case class ScalaStyle(s1: String, l1: Long, o1: Option[Any], o2: Option[Int] = Some(2), d: Option[String] = Some("d")) {
  def mkString: String = s"$s1,$l1,${o1.mkString},${o2.getOrElse("o2")},${d.mkString}"
}

class ApiCall(url: String, params: Map[_, _]) {
  def get(): Any = ???
}
