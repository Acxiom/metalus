package com.acxiom.metalus.context

import com.acxiom.metalus.{ClassInfo, Constants, DefaultCredentialProvider, MockDefaultParam, MockNoParams, PipelineStateInfo, PipelineStepResponse}
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager
import java.util.{Properties, UUID}

class SessionContextTests extends AnyFunSpec {
  private val pipelineKey: PipelineStateInfo = PipelineStateInfo("pipeline", Some("step"))

  private val DEFAULT_OBJECT = new MockDefaultParam(true, "special string")

  private val GLOBALS_MAP = Map[String, Any]("param" -> DEFAULT_OBJECT,
    "string" -> "test string",
    "int" -> Constants.FIVE)

  private val BASE_STEP_AUDIT = ExecutionAudit(pipelineKey, AuditType.STEP, Map(), System.currentTimeMillis(),
    None, None)
  private val BASE_STEP_RESPONSE = PipelineStepResponse(Some("Test String"), None)

  private val FULL_STEP_RESPONSE = PipelineStepResponse(Some(DEFAULT_OBJECT),
    Some(GLOBALS_MAP))

  describe("SessionContext") {
    describe("Noop Storage") {
      it("Should perform operations without failure") {
        val sessionId = UUID.randomUUID()
        val sessionContext = DefaultSessionContext(Some(sessionId.toString), None, None)
        assert(sessionContext.sessionId.toString == sessionId.toString)
        assert(DefaultSessionContext(None, None, None).sessionId.toString != sessionId.toString)
        assert(DefaultSessionContext(None, None, None).sessionId.toString !=
          DefaultSessionContext(None, None, None).sessionId.toString)
        assert(sessionContext.saveAudit(pipelineKey, BASE_STEP_AUDIT))
        assert(sessionContext.loadAudits().isEmpty)
        assert(sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE))
        assert(sessionContext.saveStepResult(pipelineKey, FULL_STEP_RESPONSE))
        assert(sessionContext.loadStepResults().isEmpty)
        assert(sessionContext.saveStepStatus(pipelineKey, "complete"))
        assert(sessionContext.loadStepStatus().isEmpty)
        assert(sessionContext.saveGlobals(pipelineKey, GLOBALS_MAP))
        assert(sessionContext.loadGlobals(pipelineKey).isEmpty)
        assert(!sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE.copy(primaryReturn = Some(new MockNoParams()))))
      }
    }

    describe("JDBC Storage") {
      val url = "jdbc:derby:memory:test;create=true"
      val shutDownurl = "jdbc:derby:memory:test;shutdown=true"
      val properties = new Properties()
      val connectionMap = Map[String, String]("driver" -> "org.apache.derby.jdbc.EmbeddedDriver")
      connectionMap.foreach(entry => properties.put(entry._1, entry._2))
      val conn = DriverManager.getConnection(url, properties)
      val stmt = conn.createStatement
      stmt.execute("""CREATE TABLE STEP_STATUS
          |(SESSION_ID VARCHAR(64), DATE BIGINT, VERSION INTEGER, RESULT_KEY VARCHAR(2048), STATUS VARCHAR(15))""".stripMargin)
      stmt.execute("""CREATE TABLE AUDITS
          |(SESSION_ID VARCHAR(64), DATE BIGINT, VERSION INTEGER, CONVERTOR VARCHAR(2048), AUDIT_KEY VARCHAR(2048),
          |START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT, STATE BLOB)""".stripMargin)
      stmt.execute("""CREATE TABLE STEP_RESULTS
          |(SESSION_ID VARCHAR(64), DATE BIGINT, VERSION INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
          |NAME VARCHAR(512), STATE BLOB)""".stripMargin)
      stmt.execute("""CREATE TABLE GLOBALS
          |(SESSION_ID VARCHAR(64), DATE BIGINT, VERSION INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
          |NAME VARCHAR(512), STATE BLOB)""".stripMargin)

      val parameters = Map[String, Any](
        "username" -> "redonthehead",
        "password" -> "fred",
        "credential-classes" -> "com.acxiom.metalus.UserNameCredential",
        "credential-parsers" -> ",,,com.acxiom.metalus.DefaultCredentialParser")

      it ("should read and write state data to a database") {
        val sessionStorageInfo = ClassInfo(Some("com.acxiom.metalus.context.JDBCSessionStorage"),
          Some(Map("connectionString" -> url,
          "connectionProperties" -> Map("driver" -> "org.apache.derby.jdbc.EmbeddedDriver"),
          "credentialName" -> "redonthehead",
          "credentialProvider" -> new DefaultCredentialProvider(parameters))))
        val sessionContext = DefaultSessionContext(None, Some(sessionStorageInfo), None)
        // Status
        sessionContext.saveStepStatus(pipelineKey, "running")
        var results = stmt.executeQuery("SELECT * FROM STEP_STATUS")
        assert(results.next())
        assert(results.getInt("VERSION") == 0)
        assert(results.getString("STATUS") == "RUNNING")
        sessionContext.saveStepStatus(pipelineKey, "comPLete")
        results = stmt.executeQuery("SELECT * FROM STEP_STATUS")
        assert(results.next())
        assert(results.getInt("VERSION") == 0)
        assert(results.getString("STATUS") == "COMPLETE")
        val statusList = sessionContext.loadStepStatus()
        assert(statusList.isDefined)
        assert(statusList.get.length == 1)
        assert(statusList.get.head.status == "COMPLETE")
        // Audit
        // Simulate saving an audit when a step / pipeline starts
        sessionContext.saveAudit(pipelineKey, BASE_STEP_AUDIT)
        // Load the audits and verify that the proper values are stored
        var audits = sessionContext.loadAudits()
        assert(audits.isDefined && audits.get.length == 1)
        assert(audits.get.head.start == BASE_STEP_AUDIT.start)
        assert(audits.get.head.auditType == BASE_STEP_AUDIT.auditType)
        // Simulate saving an audit after a step / pipeline completes
        var updatedAudit = BASE_STEP_AUDIT.setEnd(System.currentTimeMillis())
        sessionContext.saveAudit(pipelineKey, updatedAudit)
        audits = sessionContext.loadAudits()
        assert(audits.isDefined && audits.get.length == 1)
        assert(audits.get.head.start == updatedAudit.start)
        assert(audits.get.head.end == updatedAudit.end)
        assert(audits.get.head.durationMs == updatedAudit.durationMs)
        assert(audits.get.head.auditType == updatedAudit.auditType)
        // Check the DB to determine if the record was created properly
        results = stmt.executeQuery("SELECT * FROM AUDITS")
        assert(results.next())
        assert(results.getInt("VERSION") == 0)
        assert(results.getLong("END_TIME") == updatedAudit.end.getOrElse(-Constants.FIFTEEN))
        // Save one more time to ensure we get the most recent record
        updatedAudit = BASE_STEP_AUDIT.setEnd(System.currentTimeMillis())
        sessionContext.saveAudit(pipelineKey, updatedAudit)
        audits = sessionContext.loadAudits()
        assert(audits.isDefined && audits.get.length == 1)
        assert(audits.get.head.start == updatedAudit.start)
        assert(audits.get.head.end == updatedAudit.end)
        assert(audits.get.head.durationMs == updatedAudit.durationMs)
        assert(audits.get.head.auditType == updatedAudit.auditType)
        results = stmt.executeQuery("SELECT * FROM AUDITS")
        assert(results.next())
        assert(results.getInt("VERSION") == 1)
        assert(results.getLong("END_TIME") == updatedAudit.end.getOrElse(-Constants.FIFTEEN))

        // Step Result
        assert(sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE))
        val fullStepKey = pipelineKey.copy(stepId = Some("step1"))
        assert(sessionContext.saveStepResult(fullStepKey, FULL_STEP_RESPONSE))
        val steps = sessionContext.loadStepResults()
        assert(steps.nonEmpty)
        assert(steps.get.size == 2)
        assert(steps.get.contains(pipelineKey.key))
        val step = steps.get(pipelineKey.key)
        assert(step.namedReturns.isEmpty)
        val fullStep = steps.get(fullStepKey.key)
        var defaultObjParam = fullStep.primaryReturn.get.asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)
        val namedResponse = fullStep.namedReturns
        assert(namedResponse.isDefined)
        assert(namedResponse.get.size == Constants.THREE)
        assert(namedResponse.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(namedResponse.get.getOrElse("int", -1) == GLOBALS_MAP("int"))
        defaultObjParam = namedResponse.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Globals
        assert(sessionContext.saveGlobals(pipelineKey, GLOBALS_MAP))
        val updatedMap = GLOBALS_MAP + ("int" -> Constants.SIX) + ("test" -> 50L)
        val subPipelineKey = PipelineStateInfo("my-pipeline", None, stepGroup = Some(pipelineKey))
        assert(sessionContext.saveGlobals(subPipelineKey, updatedMap))
        // Main globals
        assert(sessionContext.loadGlobals(pipelineKey).nonEmpty)
        assert(namedResponse.get.size == Constants.THREE)
        assert(namedResponse.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(namedResponse.get.getOrElse("int", -1) == GLOBALS_MAP("int"))
        defaultObjParam = namedResponse.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Second Globals
        var subGlobals = sessionContext.loadGlobals(subPipelineKey)
        assert(subGlobals.nonEmpty)
        assert(subGlobals.get.size == Constants.FOUR)
        assert(subGlobals.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(subGlobals.get.getOrElse("test", -1L) == 50L)
        assert(subGlobals.get.getOrElse("int", -1) == Constants.SIX)
        defaultObjParam = subGlobals.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Save the globals again to get another version
        assert(sessionContext.saveGlobals(subPipelineKey, updatedMap + ("test" -> 60L)))
        subGlobals = sessionContext.loadGlobals(subPipelineKey)
        assert(subGlobals.nonEmpty)
        assert(subGlobals.get.size == Constants.FOUR)
        assert(subGlobals.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(subGlobals.get.getOrElse("test", -1L) == 60L)
        assert(subGlobals.get.getOrElse("int", -1) == Constants.SIX)
        // Check the DB to determine if the record was created properly
        results = stmt.executeQuery(
          s"""SELECT * FROM GLOBALS WHERE SESSION_ID = '${sessionContext.sessionId.toString}'
             |AND RESULT_KEY = '${subPipelineKey.key}'
             |AND NAME = 'test'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 1)

        try {
          stmt.close()
          conn.close()
          DriverManager.getConnection(shutDownurl)
        } catch {
          case _ => // Do nothing
        }
      }
    }
  }
}
