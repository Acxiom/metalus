package com.acxiom.metalus.sql.parser

import com.acxiom.metalus.sql._
import com.acxiom.metalus.utils.ReflectionUtils
import org.scalatest.funspec.AnyFunSpec

class SqlExpressionParserTests extends AnyFunSpec {

  // dummy raw value, since we're not comparing on this
  implicit val raw: () => String = () => ""

  implicit class ExpressionStringContextImplicits(sc: StringContext) {
    def i(args: Any*): Identifier = {
      val stringVal = sc.s(args: _*)
      val quote = if (stringVal.head == '`' || stringVal.head == '"') Some(stringVal.take(1)) else None
      val identifiers = stringVal.split('.').map(_.replaceAllLiterally("`", "").replaceAllLiterally("\"", "")).toList
      Identifier(identifiers.last, if(identifiers.size > 1) Some(identifiers.dropRight(1)) else None, quote)(stringVal)
    }
    def l(args: Any*): Literal = {
      val stringVal = sc.s(args: _*)
      val quote = if(stringVal.head == '\'') Some("'") else None
      Literal(stringVal.stripPrefix("'").stripSuffix("'"), quote)(stringVal)
    }
  }

  describe("SqlExpressionParser - Basic") {
    val basicTests = List(
      // identifier
      ("name", i"name"),
      ("`name`", i"`name`"),
      ("chickens.name", i"chickens.name"),
      ("chickens.\"bad name\"", Identifier("bad name", Some(List("chickens")), Some("\""))("chickens.\"bad name\"")),
      // alias
      ("breeds.name as breed_name", Alias(i"breeds.name", i"breed_name")),
      // unary expressions
      ("-1", UnaryOperator(l"1", "-")),
      ("+2", UnaryOperator(l"2", "+")),
      ("~3", UnaryOperator(l"3", "~")),
      ("NOT chickens.rooster", LogicalNot(i"chickens.rooster")),
      // simple binary expressions
      ("chickens.name = 'cogburn'", BinaryOperator(i"chickens.name", l"'cogburn'", "=")),
      ("chickens.name IS NOT NULL", BinaryOperator(i"chickens.name", LogicalNot(l"NULL"), "IS")),
      ("chickens.rooster IS NOT FALSE", BinaryOperator(i"chickens.rooster", LogicalNot(l"FALSE"), "IS")),
      ("chickens.age + 1", BinaryOperator(i"chickens.age", l"1", "+")),
      // nested binary expressions
      ("'chickens' || ' ' || 'rule!'", BinaryOperator(BinaryOperator(l"'chickens'", l"' '", "||"), l"'rule!'", "||")),
      ("a > 1 AND breed NOT LIKE '%orp%'",
        BinaryOperator(BinaryOperator(i"a", l"1", ">"), Like(i"breed", l"'%orp%'", not = true, None), "AND")),
      // sub query
      ("(select id as breed_id from breeds)", SubQuery("select id as breed_id from breeds")),
      ("EXISTS (select 1 from breeds b where b.id = breed_id)",
        Exists(SubQuery("select 1 from breeds b where b.id = breed_id"))),
      // case
      ("case when id = 1 then 'gamecock' else 'silkie' end",
        Case(List(When(BinaryOperator(i"id", l"1", "="), l"'gamecock'")), None, Some(l"'silkie'"))),
      ("case chickens.breed_id when 1 then 'gamecock' else 'silkie' end",
        Case(List(When(l"1", l"'gamecock'")), Some(i"chickens.breed_id"), Some(l"'silkie'"))),
      ("IF (chickens.rooster) 'rooster' ELSE 'hen'",
        Case(List(When(i"chickens.rooster", l"'rooster'")), None, Some(l"'hen'"))),
      // raw
      ("RAW('strange syntax')", Raw("strange syntax"))
    )
    it("should parse expressions") {
      basicTests.foreach { case (test, expected) =>
        assert(SqlExpressionParser.parse(test) == expected, test)
      }
    }

    it ("should allow for transformations") {
      val expr = SqlExpressionParser.parse("a > 1 AND breed NOT LIKE '%orp%'")
      val transformed = expr.transform{
        case l: Like => l.copy(not = !l.not)(l.getRaw)
      }
      assert(transformed.exists({
        case l: Like if !l.not => true
        case _ => false
      }))
    }

    it("should walk a parse tree") {
      val expr = SqlExpressionParser.parse("1 + 2 + 3 + 4 + 5")
      val res = expr.walk(List[Int]())(exit = {
        case (stack, NumericLiteral(l)) => l.value.toInt +: stack
        case (stack, BinaryOperator(_, _, "+")) => stack.take(2).sum +: stack.drop(2)
      })
      assert(res.nonEmpty)
      assert(res.size == 1)
      assert(res.head == "15".toInt)
    }
  }

}
