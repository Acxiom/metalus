package com.acxiom.metalus.sql.jdbc

import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.sql._
import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineStateInfo, PipelineStepMapper}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

class JDBCDataReferenceTests extends AnyFunSpec with BeforeAndAfterAll with GivenWhenThen {

  lazy val pipelineContext: PipelineContext = {
    PipelineContext(Some(Map[String, Any]()),
      List(PipelineParameter(PipelineStateInfo("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateInfo("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
  }

  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)

    pipelineContext
  }

  describe("JDBCDataReference - Basic") {
    val chickens = BasicJDBCDataReference("chickens", "jdbc:derby", Map(), pipelineContext = pipelineContext)
    val breeds = BasicJDBCDataReference("breeds", "jdbc:derby", Map(), pipelineContext = pipelineContext)
    it("should build a select statement") {
      val query = chickens + As("c") + Where("c.breed = 'silkie'") +
        Join(breeds + As("b"), "inner", Some("c.breed = b.breed"), None)
      println(chickens.toSql)
      println(query.asInstanceOf[JDBCDataReference[_]].toSql)
    }
  }

}
