package com.acxiom.metalus.context

import com.acxiom.metalus.{Constants, MockDefaultParam, MockNoParams, PipelineStateInfo, PipelineStepResponse}
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import org.scalatest.funspec.AnyFunSpec

import java.util.UUID

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
        assert(sessionContext.saveGlobals(pipelineKey, GLOBALS_MAP))
        assert(sessionContext.loadGlobals(pipelineKey).isEmpty)
        assert(!sessionContext.saveStepResult(pipelineKey, BASE_STEP_RESPONSE.copy(primaryReturn = Some(new MockNoParams()))))
      }
    }

    describe("JDBC Storage") {

    }
  }
}
