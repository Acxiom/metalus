package com.acxiom.metalus.steps

import com.acxiom.metalus.{Constants, PipelineListener, RetryPolicy, TestHelper}
import com.acxiom.metalus.connectors.InMemoryDataConnector
import com.acxiom.metalus.sql.{Attribute, AttributeType, InMemoryDataReference, InMemoryTable, Row, Schema}
import org.scalatest.funspec.AnyFunSpec

import scala.io.Source

class QueryStepsTests extends AnyFunSpec {
  describe("QuerySteps") {
    describe("runSQL") {
      it("should run a query against an existing DataReference") {
        val schema = Schema(Seq(
          Attribute("ID", AttributeType("int"), None, None),
          Attribute("FIRST_NAME", AttributeType("String"), None, None),
          Attribute("LAST_NAME", AttributeType("String"), None, None),
          Attribute("EMAIL", AttributeType("String"), None, None),
          Attribute("GENDER", AttributeType("String"), None, None),
          Attribute("EIN", AttributeType("String"), None, None),
          Attribute("POSTAL_CODE", AttributeType("String"), None, None)
        ))
        val rows = Source.fromInputStream(getClass.getResourceAsStream("/MOCK_DATA.csv")).getLines().drop(1).toList.map(line => {
          Row(line.split(','), Some(schema), Some(line))
        })
        val properties = Map("data" -> rows.map(_.columns), "schema" -> schema)
        TestHelper.pipelineListener = PipelineListener()
        val pipelineContext = TestHelper.generatePipelineContext()
        val dataRef = InMemoryDataConnector("data-chunk")
          .createDataReference(Some(properties), pipelineContext)
          .asInstanceOf[InMemoryDataReference]
        val updateRef = QueryingSteps.runSQL("select LAST_NAME, FIRST_NAME, GENDER from !stinkyPete WHERE LAST_NAME = 'Betancourt'",
          "stinkyPete", dataRef, RetryPolicy(Some(Constants.ZERO)), pipelineContext)
        val table = updateRef.execute.asInstanceOf[InMemoryTable]
        assert(table.data.length == Constants.TWO)
        assert(table.data.head.length == Constants.THREE)
        assert(table.data(Constants.ONE).length == Constants.THREE)
        table.data.foreach(row => {
          row(Constants.ONE).toString match {
            case "Matteo" =>
              assert(row(Constants.ZERO).toString == "Betancourt")
              assert(row(Constants.TWO).toString == "Male")
            case "Kessia" =>
              assert(row(Constants.ZERO).toString == "Betancourt")
              assert(row(Constants.TWO).toString == "Female")
          }
        })
      }
    }
  }
}
