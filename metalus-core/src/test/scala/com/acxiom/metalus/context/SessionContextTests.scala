package com.acxiom.metalus.context

import com.acxiom.metalus._
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import com.acxiom.metalus.fs.{LocalFileManager, SFTPFileManager}
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager
import java.util.UUID

// noinspection SqlNoDataSourceInspection
class SessionContextTests extends AnyFunSpec {
  private val pipelineKey: PipelineStateKey = PipelineStateKey("pipeline", Some("step"))

  private val DEFAULT_OBJECT = new MockDefaultParam(true, "special string")

  private val GLOBALS_MAP = Map[String, Any]("param" -> DEFAULT_OBJECT,
    "string" -> "test string",
    "int" -> Constants.FIVE,
    "fileManager" ->
      SFTPFileManager("fake.host.com", Some(Constants.SIXTY), Some("nopasswduser"),
        config = Some(Map("key" -> "is useless"))))

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
        assert(sessionContext.startSession())
        assert(sessionContext.completeSession("COMPLETE"))
        assert(sessionContext.sessionHistory.isEmpty)
        assert(sessionContext.saveAudit(pipelineKey, BASE_STEP_AUDIT))
        assert(sessionContext.loadAudits().isEmpty)
        assert(sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE))
        assert(sessionContext.saveStepResult(pipelineKey, FULL_STEP_RESPONSE))
        assert(sessionContext.loadStepResults().isEmpty)
        assert(sessionContext.saveStepStatus(pipelineKey, "complete", None))
        assert(sessionContext.loadStepStatus().isEmpty)
        assert(sessionContext.saveGlobals(pipelineKey, GLOBALS_MAP))
        assert(sessionContext.loadGlobals(pipelineKey).isEmpty)
        assert(!sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE.copy(primaryReturn = Some(new MockNoParams()))))
      }
    }

    describe("JDBC Storage") {
      val settings = TestHelper.setupSessionTestDB("sessionTest")
      val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
      val stmt = conn.createStatement

      it("should read and write state data to a database") {
        val sessionStorageInfo = JDBCSessionStorage(settings.url, Map("driver" -> "org.apache.derby.jdbc.EmbeddedDriver"),
          None, Some(TestHelper.getDefaultCredentialProvider))
        val sessionContext = DefaultSessionContext(None, Some(sessionStorageInfo), None)
        // Application
        var results = stmt.executeQuery("SELECT * FROM SESSIONS")
        assert(!results.next())
        results = stmt.executeQuery("SELECT * FROM SESSION_HISTORY")
        assert(!results.next())
        assert(sessionContext.startSession())
        results = stmt.executeQuery("SELECT * FROM SESSIONS")
        assert(results.next())
        assert(results.getString("STATUS") == "RUNNING")
        results = stmt.executeQuery("SELECT * FROM SESSION_HISTORY")
        assert(!results.next())
        assert(sessionContext.completeSession("COMplETE"))
        results = stmt.executeQuery("SELECT * FROM SESSIONS")
        assert(results.next())
        assert(results.getString("STATUS") == "COMPLETE")
        results = stmt.executeQuery("SELECT * FROM SESSION_HISTORY")
        assert(!results.next())
        assert(sessionContext.startSession())
        results = stmt.executeQuery("SELECT * FROM SESSIONS")
        assert(results.next())
        assert(results.getLong("END_TIME") == -1)
        assert(results.getString("STATUS") == "RUNNING")
        results = stmt.executeQuery("SELECT * FROM SESSION_HISTORY")
        assert(results.next())
        val applicationHistory = sessionContext.sessionHistory
        assert(applicationHistory.isDefined)
        assert(applicationHistory.get.length == Constants.TWO)
        assert(applicationHistory.get.head.status == "RUNNING")
        // Status
        sessionContext.saveStepStatus(pipelineKey, "running", None)
        results = stmt.executeQuery("SELECT * FROM STEP_STATUS")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.ONE)
        assert(results.getString("STATUS") == "RUNNING")
        sessionContext.saveStepStatus(pipelineKey, "comPLete", Some(List("nextstep1", "nextstep2")))
        results = stmt.executeQuery("SELECT * FROM STEP_STATUS")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.ONE)
        assert(results.getString("STATUS") == "COMPLETE")
        val statusList = sessionContext.loadStepStatus()
        assert(statusList.isDefined)
        assert(statusList.get.length == 1)
        assert(statusList.get.head.status == "COMPLETE")
        assert(statusList.get.head.nextSteps.isDefined)
        assert(statusList.get.head.nextSteps.get.length == Constants.TWO)
        assert(statusList.get.head.nextSteps.get.contains("nextstep1"))
        assert(statusList.get.head.nextSteps.get.contains("nextstep2"))
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
        assert(results.getInt("RUN_ID") == Constants.ONE)
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
        assert(results.getInt("RUN_ID") == 1)
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
        assert(namedResponse.get.size == Constants.FOUR)
        assert(namedResponse.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(namedResponse.get.getOrElse("int", -1) == GLOBALS_MAP("int"))
        defaultObjParam = namedResponse.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Globals
        assert(sessionContext.saveGlobals(pipelineKey, GLOBALS_MAP))
        val updatedMap = GLOBALS_MAP + ("int" -> Constants.SIX) + ("test" -> 50L)
        val subPipelineKey = PipelineStateKey("my-pipeline", None, stepGroup = Some(pipelineKey))
        assert(sessionContext.saveGlobals(subPipelineKey, updatedMap))
        // Main globals
        val restoredGlobals = sessionContext.loadGlobals(pipelineKey)
        assert(restoredGlobals.nonEmpty)
        assert(restoredGlobals.get.contains("fileManager"))
        assert(restoredGlobals.get("fileManager").isInstanceOf[SFTPFileManager])
        val fileManager = restoredGlobals.get("fileManager").asInstanceOf[SFTPFileManager]
        assert(fileManager.hostName == "fake.host.com")
        assert(fileManager.port.getOrElse(Constants.ZERO) == Constants.SIXTY)
        assert(fileManager.user.getOrElse("") == "nopasswduser")
        assert(fileManager.password.isEmpty)
        assert(fileManager.knownHosts.isEmpty)
        assert(fileManager.bulkRequests.isEmpty)
        assert(fileManager.timeout.isEmpty)
        assert(fileManager.config.isDefined)
        assert(fileManager.config.get.size == Constants.ONE)
        assert(fileManager.config.get.contains("key"))
        assert(fileManager.config.get("key") == "is useless")
        assert(namedResponse.get.size == Constants.FOUR)
        assert(namedResponse.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(namedResponse.get.getOrElse("int", -1) == GLOBALS_MAP("int"))
        defaultObjParam = namedResponse.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Second Globals
        var subGlobals = sessionContext.loadGlobals(subPipelineKey)
        assert(subGlobals.nonEmpty)
        assert(subGlobals.get.size == Constants.FIVE)
        assert(subGlobals.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(subGlobals.get.getOrElse("test", -1L) == 50L)
        assert(subGlobals.get.getOrElse("int", -1) == Constants.SIX)
        defaultObjParam = subGlobals.get("param").asInstanceOf[MockDefaultParam]
        assert(defaultObjParam.getFlag == DEFAULT_OBJECT.getFlag)
        assert(defaultObjParam.getSecondParam == DEFAULT_OBJECT.getSecondParam)

        // Save the globals again to get another RUN_ID
        assert(sessionContext.saveGlobals(subPipelineKey, updatedMap + ("test" -> 60L)))
        subGlobals = sessionContext.loadGlobals(subPipelineKey)
        assert(subGlobals.nonEmpty)
        assert(subGlobals.get.size == Constants.FIVE)
        assert(subGlobals.get.getOrElse("string", "BAD") == GLOBALS_MAP("string"))
        assert(subGlobals.get.getOrElse("test", -1L) == 60L)
        assert(subGlobals.get.getOrElse("int", -1) == Constants.SIX)
        // Check the DB to determine if the record was created properly
        results = stmt.executeQuery(
          s"""SELECT * FROM GLOBALS WHERE SESSION_ID = '${sessionContext.sessionId.toString}'
             |AND RESULT_KEY = '${subPipelineKey.key}'
             |AND NAME = 'test'""".stripMargin)
        assert(results.next())
        assert(results.getInt("RUN_ID") == 1)

        // noinspection DangerousCatchAll
        try {
          stmt.close()
          conn.close()
          TestHelper.stopTestDB(settings.name)
        } catch {
          case _ => // Do nothing
        }
      }
    }
  }
}
