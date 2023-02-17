package com.acxiom.metalus.sql.parser

import org.json4s.{DefaultFormats, Formats}
import org.scalatest.funspec.AnyFunSpec

class SqlParserTests extends AnyFunSpec {

  implicit val format: Formats = DefaultFormats

  describe("SqlParser - Basic") {
    it("should parse a select statement into a pipeline") {
//      val text =
//        """select * FROM (SELECT c.name, c.breed_id AS breeds FROM @chickens c
//          |WHERE c.name = 'cogburn') fc
//          |WHERE fc.breeds > 2""".stripMargin
          val text =
            """SELECT c.name, b.name as breed FROM @chickens c
              |JOIN @breeds b on c.breed_id = b.id
              |""".stripMargin
      val steps = SqlParser.parse(text)
      println(org.json4s.native.Serialization.write(steps))
    }
  }

}
