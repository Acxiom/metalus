package com.acxiom.metalus

import com.acxiom.metalus.context.ContextManager

import java.sql.DriverManager
import java.util.Properties

object TestHelper {
  var pipelineListener: PipelineListener = _

  def generatePipelineContext(): PipelineContext = {
    val parameters = Map[String, Any]()
    PipelineContext(Some(parameters),
      List[PipelineParameter](),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      }
      else {
        List("com.acxiom.metalus", "com.acxiom.metalus.steps")
      }),
      PipelineStepMapper(),
      Some(TestHelper.pipelineListener),
      List(), PipelineManager(List()), None, new ContextManager(Map(), Map()))
  }

  def getDefaultCredentialProvider: CredentialProvider = {
    val parameters = Map[String, Any](
      "username" -> "redonthehead",
      "password" -> "fred",
      "credential-classes" -> "com.acxiom.metalus.UserNameCredential",
      "credential-parsers" -> ",,,com.acxiom.metalus.DefaultCredentialParser")
    new DefaultCredentialProvider(parameters)
  }

  def setupTestDB(name: String): MemoryDBSettings = {
    val url = s"jdbc:derby:memory:$name;create=true"
    val properties = new Properties()
    val connectionMap = Map[String, String]("driver" -> "org.apache.derby.jdbc.EmbeddedDriver")
    connectionMap.foreach(entry => properties.put(entry._1, entry._2))
    val conn = DriverManager.getConnection(url, properties)
    val stmt = conn.createStatement
    stmt.execute(
      """CREATE TABLE STEP_STATUS
        |(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, RESULT_KEY VARCHAR(2048), STATUS VARCHAR(15))""".stripMargin)
    stmt.execute(
      """CREATE TABLE STEP_STATUS_STEPS
        |(SESSION_ID VARCHAR(64), RUN_ID INTEGER, RESULT_KEY VARCHAR(2048), STEP_ID VARCHAR(2048))""".stripMargin)
    stmt.execute(
      """CREATE TABLE AUDITS
        |(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), AUDIT_KEY VARCHAR(2048),
        |START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT, STATE BLOB)""".stripMargin)
    stmt.execute(
      """CREATE TABLE STEP_RESULTS
        |(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
        |NAME VARCHAR(512), STATE BLOB)""".stripMargin)
    stmt.execute(
      """CREATE TABLE GLOBALS
        |(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
        |NAME VARCHAR(512), STATE BLOB)""".stripMargin)
    stmt.execute(
      """CREATE TABLE SESSIONS
        |(SESSION_ID VARCHAR(64), RUN_ID INTEGER, STATUS VARCHAR(15), START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT)""".stripMargin)
    stmt.execute(
      """CREATE TABLE SESSION_HISTORY
        |(SESSION_ID VARCHAR(64), RUN_ID INTEGER,  STATUS VARCHAR(15), START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT)""".stripMargin)
    MemoryDBSettings(name, url, properties)
  }

  def stopTestDB(name: String): Unit = {
    try {
      DriverManager.getConnection(s"jdbc:derby:memory:$name;shutdown=true")
    } catch {
      case _ => // Do nothing
    }
  }
}

case class MemoryDBSettings(name: String, url: String, connectionProperties: Properties)
