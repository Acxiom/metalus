package com.acxiom.metalus.sql.jdbc

import com.acxiom.metalus.connectors.jdbc.JDBCDataConnector
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.sql._
import com.acxiom.metalus.{DefaultPipelineListener, PipelineContext, PipelineParameter, PipelineStateKey, PipelineStepMapper, UserNameCredential}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import java.nio.file.{Files, Path}
import java.sql.DriverManager

class JDBCDataReferenceTests extends AnyFunSpec with BeforeAndAfterAll with GivenWhenThen {

  lazy val pipelineContext: PipelineContext = {
    PipelineContext(Some(Map[String, Any]()),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      contextManager = new ContextManager(Map(), Map()))
  }

  val url = "jdbc:derby:memory:test"
  val localDir: Path = Files.createTempDirectory("sparkLocal")
  val properties: Map[String, String] = Map(
    "driver" -> "org.apache.derby.jdbc.EmbeddedDriver"
  )

  val credential: UserNameCredential = UserNameCredential(Map("username" -> "test_fixture", "password" -> "chicken"))

  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)
    System.setProperty("derby.system.home", localDir.toFile.getAbsolutePath + "/.derby")
    val con = DriverManager.getConnection(s"$url;user=test_fixture;password=chicken;create=true")
    val st = con.createStatement()

    // create chickens
    st.executeUpdate("CREATE TABLE CHICKENS (ID INT PRIMARY KEY, NAME VARCHAR(100), BREED_ID INT)")
    st.executeUpdate("INSERT INTO CHICKENS VALUES (1, 'Cogburn', 1)")
    st.executeUpdate("INSERT INTO CHICKENS VALUES (2, 'Gold Hen', 2)")
    st.executeUpdate("INSERT INTO CHICKENS VALUES (3, 'Black Hen', 2)")
    st.executeUpdate("INSERT INTO CHICKENS VALUES (4, 'Fluffy', 3)")

    // create breeds
    st.executeUpdate("CREATE TABLE BREEDS (ID INT PRIMARY KEY, NAME VARCHAR(100))")
    st.executeUpdate("INSERT INTO BREEDS VALUES (1, 'Game Cock')")
    st.executeUpdate("INSERT INTO BREEDS VALUES (2, 'Sex-link')")
    st.executeUpdate("INSERT INTO BREEDS VALUES (3, 'Silkie')")
    st.executeUpdate("CREATE TABLE B2 AS SELECT NAME FROM BREEDS WITH NO DATA")
    st.close()
    con.close()
    pipelineContext
  }

  describe("JDBCDataReference - Basic") {
    val conn = JDBCDataConnector(url, "testJDBC", None, Some(credential))
    val chickens = conn.getTable("chickens", Some(properties), pipelineContext)
    val breeds = conn.getTable("breeds", Some(properties), pipelineContext)
    it("should execute a select statement") {
      val query = chickens + As("c") +
        Join(breeds + As("b"), "inner", Some(Expression("c.breed_id = b.id")), None) +
        Where(Expression("b.name IN ('Game Cock', 'Sex-link')")) +
        Select(List(Expression("c.name as chicken"), Expression("b.name as breed"))) +
        GroupBy(List(Expression("breed"))) +
        Select(List(Expression("count(breed) AS breed_count"), Expression("breed")))
      val jdbcResult = query.asInstanceOf[JDBCDataReference[JDBCResult]].execute
      assert(jdbcResult.resultSet.isDefined)
      val rows = jdbcResult.resultSet.get
      assert(rows.size == 2)
      assert(rows.exists(m => m.get("BREED").exists(_.toString == "Game Cock")
        && m.get("BREED_COUNT").exists(_.toString == "1")))
      assert(rows.exists(m => m.get("BREED").exists(_.toString == "Sex-link")
        && m.get("BREED_COUNT").exists(_.toString == "2")))
    }

    it("should create a table") {
      val createCounts = chickens + As("c") +
        Join(breeds + As("b"), "inner", Some(Expression("c.breed_id = b.id")), None) +
        Where(Expression("b.name IN ('Game Cock', 'Sex-link')")) +
        Select(List(Expression("c.name as chicken"), Expression("b.name as breed"))) +
        GroupBy(List(Expression("breed"))) +
        Select(List(Expression("count(breed) AS breed_count"), Expression("breed"))) +
        CreateAs("breed_counts", noData = true)
      val jdbcResult = createCounts.asInstanceOf[JDBCDataReference[JDBCResult]].execute
      assert(jdbcResult.resultSet.isEmpty)
      val query = conn.getTable("breed_counts", Some(properties), pipelineContext)
      val res = query.asInstanceOf[JDBCDataReference[JDBCResult]].execute
      assert(res.resultSet.isDefined)
    }
  }

}
