package com.acxiom.metalus.sql.parser

import org.json4s.{DefaultFormats, Formats}
import org.scalatest.funspec.AnyFunSpec

class SqlParserTests extends AnyFunSpec {

  implicit val format: Formats = DefaultFormats

  describe("SqlParser - Basic") {
    it("should parse a select statement into a pipeline") {
      val text =
        """SELECT c.name, b.name as breed FROM @chickens c
          |JOIN @breeds b on c.breed_id = b.id
          |""".stripMargin
      val steps = SqlParser.parse(text)
      assert(steps.nonEmpty)
      assert(steps.last.id.mkString == "SELECT_c_b")
    }

    it("should parse an update statement into a pipeline") {
      val text =
        """UPDATE @chickens
          |SET breed_id = 1
          |WHERE name = 'cogburn'
          |""".stripMargin
      val steps = SqlParser.parse(text)
      assert(steps.nonEmpty)
    }
  }

}
